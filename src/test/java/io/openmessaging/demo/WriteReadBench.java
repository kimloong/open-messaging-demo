package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

/**
 * Created by KimLoong on 17-7-2.
 */
public class WriteReadBench {private static int[] lengths = new int[]{1024, 32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024};
    private static String fileName = "E://read-write-test";
    private static int times = 10_000;

    public static void main(String[] args) throws IOException {
        Random random = new Random();
        for (int length : lengths) {
            byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + length);
            File file = new File(fileName);
            if (file.exists()) {
                file.delete();
            }
            file.createNewFile();
            //write
            RandomAccessFile writeAccessFile = new RandomAccessFile(file, "rw");
            FileChannel writeFileChannel = writeAccessFile.getChannel();
            long writeStart = System.currentTimeMillis();
            for (int i = 0; i < times; i++) {
                writeOnce(length, bytes, byteBuffer, writeFileChannel);
//                writeTwice(length, bytes, byteBuffer, writeFileChannel);
            }
            long writeCost = System.currentTimeMillis() - writeStart;
            System.out.printf("write length:%3d cost:%5d ms,times:%6d\n", length / 1024, writeCost, times);
            writeFileChannel.close();
            writeAccessFile.close();

            //read
            RandomAccessFile readAccessFile = new RandomAccessFile(file, "rw");
            FileChannel readFileChannel = readAccessFile.getChannel();
            long readStart = System.currentTimeMillis();
            for (int i = 0; i < times; i++) {
                readOnce(length, byteBuffer, readFileChannel);
//                readTwice(length, byteBuffer, readFileChannel);
            }
            long readCost = System.currentTimeMillis() - readStart;
            System.out.printf("read  length:%3d cost:%5d ms,times:%6d\n", length / 1024, readCost, times);
            readFileChannel.close();
            readAccessFile.close();

            file.delete();
        }
    }

    private static void writeOnce(int length, byte[] bytes, ByteBuffer byteBuffer, FileChannel fileChannel) throws IOException {
        byteBuffer.clear();
        byteBuffer.putInt(length);
        byteBuffer.put(bytes);
        byteBuffer.flip();
        fileChannel.write(byteBuffer);
    }

    private static void readOnce(int length, ByteBuffer byteBuffer, FileChannel readFileChannel) throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(4 + length);
        readFileChannel.read(byteBuffer);
    }

    private static void writeTwice(int length, byte[] bytes, ByteBuffer byteBuffer, FileChannel fileChannel) throws IOException {
        byteBuffer.clear();
        byteBuffer.putInt(length);
        byteBuffer.flip();
        fileChannel.write(byteBuffer);
        fileChannel.write(ByteBuffer.wrap(bytes));
    }

    private static void readTwice(int length, ByteBuffer byteBuffer, FileChannel readFileChannel) throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(4);
        readFileChannel.read(byteBuffer);
        byteBuffer.limit(4 + length);
        readFileChannel.read(byteBuffer);
    }
}