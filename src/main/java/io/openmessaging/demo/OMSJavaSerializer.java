package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.exception.OMSRuntimeException;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * 使用JAVA内建的序列化方式
 * Created by KimLoong on 17-6-28.
 */
public class OMSJavaSerializer implements OMSSerializer{

    public void serializeMessage(Message message,ByteBuffer byteBuffer) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            outputStream.writeObject(message);
            byte[] messageBytes = byteArrayOutputStream.toByteArray();
            byteBuffer.putInt(messageBytes.length);
            byteBuffer.put(messageBytes);
            byteBuffer.flip();
        } catch (IOException e) {
            e.printStackTrace();
            throw new OMSRuntimeException();
        }
    }

    public Message deserializeMessage(ByteBuffer byteBuffer) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
                byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
        try (ObjectInputStream inputStream = new ObjectInputStream(byteArrayInputStream)) {
            return (Message) inputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new OMSRuntimeException();
        }
    }
}
