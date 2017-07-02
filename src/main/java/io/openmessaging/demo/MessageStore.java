package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;
import io.openmessaging.exception.OMSRuntimeException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    private static final int BYTE_BUFFER_POOL_SIZE = 20;
    private static final int META_SIZE = 4;
    private static final int MAX_MESSAGE_SIZE = 256 * 1024;

    private final Map<String, RandomAccessFile> writeAccessMap = new HashMap<>();

    private final Map<String, RandomAccessFile> readAccessMap = new ConcurrentHashMap<>();
    private final BlockingQueue<ByteBuffer> byteBufferPool = new LinkedBlockingDeque<>(BYTE_BUFFER_POOL_SIZE);
    private String storePath;
    private OMSSerializer serializer = new OMSCustomSerializer();

    public MessageStore() {
        for (int i = 0; i < BYTE_BUFFER_POOL_SIZE; i++) {
            byteBufferPool.add(ByteBuffer.allocate(MAX_MESSAGE_SIZE + META_SIZE));
        }
    }

    public static MessageStore getInstance(KeyValue properties) {
        INSTANCE.storePath = properties.getString("STORE_PATH");
        return INSTANCE;
    }

    public void putMessage(String bucket, Message message) {
        RandomAccessFile writeAccess;
        FileChannel writeChannel;
        ByteBuffer writeByteBuffer = null;
        try {
            writeAccess = writeAccessMap.get(bucket);
            if (null == writeAccess) {
                synchronized (writeAccessMap) {
                    writeAccess = writeAccessMap.get(bucket);
                    if (null == writeAccess) {
                        File file = new File(storePath, bucket);
                        writeAccess = new RandomAccessFile(file, "rw");
                        if (!file.createNewFile()) {
                            writeAccess.seek(file.length());
                        }
                        writeAccessMap.put(bucket, writeAccess);
                    }
                }
            }


            synchronized (writeAccess) {
                writeChannel = writeAccess.getChannel();

                writeByteBuffer = byteBufferPool.take();
                writeByteBuffer.clear();
                serializer.serializeMessage(message,writeByteBuffer);

                writeChannel.write(writeByteBuffer);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            throw new OMSRuntimeException();
        } finally {
            if (null != writeByteBuffer) {
                byteBufferPool.add(writeByteBuffer);
            }
        }
    }

    public Message pullMessage(String queue, String bucket) {
        RandomAccessFile readAccess = null;
        FileChannel readChannel = null;
        ByteBuffer readByteBuffer = null;
        try {
            String key = queue + "@" + bucket;
            readAccess = readAccessMap.get(key);
            if (null == readAccess) {
                synchronized (readAccessMap) {
                    readAccess = readAccessMap.get(key);
                    if (null == readAccess) {
                        File file = new File(storePath, bucket);
                        if (!file.exists()) {
                            return null;
                        }
                        readAccess = new RandomAccessFile(file, "r");
                        readAccessMap.put(key, readAccess);
                    }
                }
            }
            synchronized (readAccess) {
                readChannel = readAccess.getChannel();

                readByteBuffer = byteBufferPool.take();

                readByteBuffer.clear();
                readByteBuffer.limit(4);
                readChannel.read(readByteBuffer);
                if (readByteBuffer.position() == 0) {
                    return null;
                }
                readByteBuffer.flip();
                int length = readByteBuffer.getInt();

                readByteBuffer.clear();
                readByteBuffer.limit(length);
                readChannel.read(readByteBuffer);
                readByteBuffer.flip();
            }
            return serializer.deserializeMessage(readByteBuffer);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            throw new OMSRuntimeException();
        } finally {
            if (null != readByteBuffer) {
                byteBufferPool.add(readByteBuffer);
            }
        }
    }
}
