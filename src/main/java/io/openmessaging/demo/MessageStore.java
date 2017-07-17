package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.exception.OMSRuntimeException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    private static final int BUFFER_SIZE = 64 * 1024 * 1024;
    private static final int MAX_MESSAGE_SIZE = 256 * 1024;

    private final Map<String, RandomAccessFile> writeAccessMap = new HashMap<>();
    private final Map<String, MappedByteBuffer> writeBufferMap = new HashMap<>();

    private final Map<String, RandomAccessFile> readAccessMap = new HashMap<>();
    private final Map<String, MappedByteBuffer> readBufferMap = new HashMap<>();

    private String storePath;
    private OMSSerializer serializer = new OMSCustomSerializer(MAX_MESSAGE_SIZE);

    public MessageStore() {
    }

    public static MessageStore getInstance(KeyValue properties) {
        INSTANCE.storePath = properties.getString("STORE_PATH");
        return INSTANCE;
    }

    public void putMessage(String bucket, Message message) {
        try {
            RandomAccessFile writeAccess = writeAccessMap.get(bucket);
            if (null == writeAccess) {
                synchronized (writeAccessMap) {
                    writeAccess = writeAccessMap.get(bucket);
                    if (null == writeAccess) {
                        File file = new File(storePath, bucket);
                        writeAccess = new RandomAccessFile(file, "rw");
                        file.createNewFile();
                        MappedByteBuffer buffer = writeAccess.getChannel().map(
                                FileChannel.MapMode.READ_WRITE, 0, BUFFER_SIZE);
                        writeBufferMap.put(bucket, buffer);
                        writeAccessMap.put(bucket, writeAccess);
                    }
                }
            }


            synchronized (writeAccess) {
                FileChannel writeChannel = writeAccess.getChannel();
                MappedByteBuffer buffer = writeBufferMap.get(bucket);

                if (buffer.remaining() < MAX_MESSAGE_SIZE) {
                    long position = writeChannel.position() + buffer.position();
                    writeChannel.position(position);
                    buffer = writeChannel.map(
                            FileChannel.MapMode.READ_WRITE, position, BUFFER_SIZE);
                    writeBufferMap.put(bucket, buffer);
                }
                serializer.serializeMessage(message, buffer);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new OMSRuntimeException();
        }
    }

    public Message pullMessage(String queue, String bucket) {
        try {
            String key = queue + "@" + bucket;
            RandomAccessFile readAccess = readAccessMap.get(key);
            if (null == readAccess) {
                synchronized (readAccessMap) {
                    readAccess = readAccessMap.get(key);
                    if (null == readAccess) {
                        File file = new File(storePath, bucket);
                        if (!file.exists()) {
                            return null;
                        }
                        readAccess = new RandomAccessFile(file, "r");
                        MappedByteBuffer buffer = readAccess.getChannel().map(
                                FileChannel.MapMode.READ_ONLY, 0, BUFFER_SIZE);
                        readBufferMap.put(key, buffer);
                        readAccessMap.put(key, readAccess);
                    }
                }
            }

            synchronized (readAccess) {
                FileChannel readChannel = readAccess.getChannel();
                MappedByteBuffer buffer = readBufferMap.get(key);

                if (buffer.remaining() < MAX_MESSAGE_SIZE) {
                    long position = readChannel.position() + buffer.position();
                    readChannel.position(position);
                    buffer = readChannel.map(
                            FileChannel.MapMode.READ_ONLY, readChannel.position(), BUFFER_SIZE);
                    readBufferMap.put(key, buffer);
                }

                int length = buffer.getInt();
                if (length == 0) {
                    return null;
                }

                return serializer.deserializeMessage(buffer, length);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new OMSRuntimeException();
        }
    }

    public void flush() {
        synchronized (this) {
            writeBufferMap.forEach((s, mappedByteBuffer) -> mappedByteBuffer.force());
        }
    }
}
