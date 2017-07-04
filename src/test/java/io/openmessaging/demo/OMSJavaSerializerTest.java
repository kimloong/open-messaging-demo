package io.openmessaging.demo;

import io.openmessaging.MessageHeader;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * OMSJavaSerializerTest
 * Created by KimLoong on 17-7-2.
 */
public class OMSJavaSerializerTest {

    private final static int[] BODY_LENGTHS = new int[]{1 * 1024, 32 * 1024, 32 * 1024, 64 * 1024, 128 * 1024};
    private final static int TIMES = 100_000;

    @Test
    public void test() {
        OMSSerializer[] serializers = new OMSSerializer[]{new OMSJavaSerializer(), new OMSCustomSerializer()};

        for (int bodyLength : BODY_LENGTHS) {
            DefaultBytesMessage message = createMessage(bodyLength);
            ByteBuffer byteBuffer = ByteBuffer.allocate(256 * 1024);

            for (OMSSerializer serializer : serializers) {
                long startTime = System.currentTimeMillis();
                for (int i = 0; i < TIMES; i++) {
                    byteBuffer.clear();
                    serializer.serializeMessage(message, byteBuffer);
                }
                long cost = System.currentTimeMillis() - startTime;

                int totalLength = byteBuffer.getInt();

                System.out.printf("%20s %12s cost(ms):%4d length(byte):%6d body_length(byte):%6d times:%6d\n",
                        serializer.getClass().getSimpleName(), "serialize", cost, totalLength, bodyLength, TIMES);

                DefaultBytesMessage deserializeMessage = null;
                startTime = System.currentTimeMillis();
                for (int i = 0; i < TIMES; i++) {
                    deserializeMessage = (DefaultBytesMessage) serializer.deserializeMessage(byteBuffer, totalLength);
                    byteBuffer.position(4);
                }
                cost = System.currentTimeMillis() - startTime;

                System.out.printf("%20s %12s cost(ms):%4d length(byte):%6d body_length(byte):%6d times:%6d\n",
                        serializer.getClass().getSimpleName(), "deserialize", cost, totalLength, bodyLength, TIMES);
                assertEquals(message, deserializeMessage);
            }
        }
    }

    private DefaultBytesMessage createMessage(int bodyLength) {
        Random random = new Random();
        byte[] bodyBytes = new byte[bodyLength];
        random.nextBytes(bodyBytes);
        DefaultBytesMessage message = new DefaultBytesMessage(bodyBytes);
        message.putHeaders(MessageHeader.MESSAGE_ID, "SOME_MESSAGE_ID");
        message.putHeaders(MessageHeader.TOPIC, "SOME_TOPIC");
        message.putHeaders(MessageHeader.QUEUE, "SOME_QUEUE");
        message.putHeaders(MessageHeader.BORN_TIMESTAMP, new Date().getTime());
        message.putHeaders(MessageHeader.BORN_HOST, "SOME_BORN_HOST");
        message.putHeaders(MessageHeader.STORE_TIMESTAMP, new Date().getTime());
        message.putHeaders(MessageHeader.STORE_HOST, "SOME_STORE_HOST");
        message.putHeaders(MessageHeader.START_TIME, new Date().getTime());
        message.putHeaders(MessageHeader.STOP_TIME, new Date().getTime());
        message.putHeaders(MessageHeader.PRIORITY, 5);
        message.putHeaders(MessageHeader.RELIABILITY, 5);
        message.putHeaders(MessageHeader.SCHEDULE_EXPRESSION, "SOME_SCHEDULE_EXPRESSION");
        message.putHeaders(MessageHeader.SHARDING_KEY, "SOME_SHARDING_KEY");
        message.putHeaders(MessageHeader.SHARDING_PARTITION, "SOME_SHARDING_PARTITION");
        message.putHeaders(MessageHeader.TRACE_ID, "SOME_TRACE_ID");

        message.putProperties("property_string", "SOME_PROPERTY_STRING");
        message.putProperties("property_int", 3600);
        message.putProperties("property_double", 3.14D);
        message.putProperties("property_long", new Date().getTime());

        return message;
    }
}