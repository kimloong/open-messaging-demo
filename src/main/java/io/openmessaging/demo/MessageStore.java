package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.exception.OMSRuntimeException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    private static final int MAX_FILE_SIZE = 128 * 1024 * 1024;
    private static final int BYTE_BUFFER_POOL_SIZE = 20;
    private static final int META_SIZE = 4;
    private static final int MAX_MESSAGE_SIZE = 256 * 1024;

    private final Map<String, RandomAccessFile> writeAccessMap = new HashMap<>();
    private final Map<String, Object> writeLocks = new HashMap<>();
    private final Map<String, Integer> writeIndexes = new HashMap<>();

    private final Map<String, RandomAccessFile> readAccessMap = new HashMap<>();
    private final Map<String, Object> readLocks = new HashMap<>();
    private final Map<String, Integer> readIndexes = new HashMap<>();


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
            writeByteBuffer = byteBufferPool.take();
            writeByteBuffer.clear();
            serializer.serializeMessage(message, writeByteBuffer);

            Object lock = getWriteLock(bucket);

            synchronized (lock) {
                writeAccess = getWriteAccess(bucket);
                writeChannel = writeAccess.getChannel();
                if (writeChannel.position() >= MAX_FILE_SIZE) {
                }
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
            Object lock = getReadLock(queue, bucket);
            synchronized (lock) {
                readAccess = getReadAccess(queue, bucket);
                if (readAccess == null) {
                    return null;
                }
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

    private Object getWriteLock(String bucket) {
        return getLock(bucket, writeLocks);
    }

    private Object getReadLock(String queue, String bucket) {
        String key = queue + "@" + bucket;
        return getLock(key, readLocks);
    }

    private Object getLock(String key, Map<String, Object> locks) {
        Object lock = locks.get(key);
        if (null == lock) {
            synchronized (locks) {
                lock = locks.get(key);
                if (null == lock) {
                    lock = new Object();
                    locks.put(key, lock);
                }
            }
        }
        return lock;
    }

    private RandomAccessFile getWriteAccess(String bucket) throws IOException {
        RandomAccessFile writeAccess;
        writeAccess = writeAccessMap.get(bucket);
        if (null == writeAccess) {
            Integer index = writeIndexes.get(bucket);
            if (null == index) {
                index = 1;
                writeIndexes.put(bucket, index);
            }

            File file = new File(storePath, bucket + "-" + index);
            writeAccess = new RandomAccessFile(file, "rw");
            if (!file.createNewFile()) {
                writeAccess.seek(file.length());
            }
            writeAccessMap.put(bucket, writeAccess);
        } else {
            FileChannel fileChannel = writeAccess.getChannel();
            if (fileChannel.position() >= MAX_FILE_SIZE) {
                Integer index = writeIndexes.get(bucket);
                index++;
                writeIndexes.put(bucket, index);

                fileChannel.close();
                writeAccess.close();

                File file = new File(storePath, bucket + "-" + index);
                writeAccess = new RandomAccessFile(file, "rw");
                if (!file.createNewFile()) {
                    writeAccess.seek(file.length());
                }
                writeAccessMap.put(bucket, writeAccess);
            }
        }
        return writeAccess;
    }

    private RandomAccessFile getReadAccess(String queue, String bucket) throws IOException {
        RandomAccessFile readAccess;
        String key = queue + "@" + bucket;
        readAccess = readAccessMap.get(key);
        if (null == readAccess) {
            Integer index = readIndexes.get(key);
            if (null == index) {
                index = 1;
                readIndexes.put(key, index);
            }

            File file = new File(storePath, bucket + "-" + index);
            if (!file.exists()) {
                return null;
            }
            readAccess = new RandomAccessFile(file, "r");
            readAccessMap.put(key, readAccess);
        } else {
            FileChannel fileChannel = readAccess.getChannel();
            if (fileChannel.position() >= MAX_FILE_SIZE) {
                Integer index = readIndexes.get(key);
                index++;
                readIndexes.put(key, index);

                File file = new File(storePath, bucket + "-" + index);
                if (!file.exists()) {
                    return null;
                }
                fileChannel.close();
                readAccess.close();
                readAccess = new RandomAccessFile(file, "r");
                readAccessMap.put(key, readAccess);
            }
        }
        return readAccess;
    }
}
