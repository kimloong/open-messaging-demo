package io.openmessaging.demo;

import io.openmessaging.Message;

import java.nio.ByteBuffer;

/**
 * Created by KimLoong on 17-6-28.
 */
public interface OMSSerializer {

    void serializeMessage(Message message,ByteBuffer buffer);

    Message deserializeMessage(ByteBuffer byteBuffer, int length);
}
