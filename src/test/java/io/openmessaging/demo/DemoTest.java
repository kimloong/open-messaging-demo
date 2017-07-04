package io.openmessaging.demo;

import io.openmessaging.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Random;

/**
 * Created by Zhang JinLong(150429) on 2017-06-28.
 */
public class DemoTest {

    public static final int TIMES = 100_000;

    public static void main(String[] args) throws IOException {
        KeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "/home/closer-pcc/om");
        Producer producer = new DefaultProducer(properties);
        String queue1 = "QUEUE1";
        String topic1 = "TOPIC1";
        Random random = new Random();
        byte[] bodyBytes = new byte[32 * 1024];
        random.nextBytes(bodyBytes);
        Message sendMessage = producer.createBytesMessageToQueue(queue1, bodyBytes);

        sendMessage.putHeaders(MessageHeader.BORN_TIMESTAMP, new Date().getTime());
        sendMessage.putHeaders(MessageHeader.BORN_HOST, "SOME_BORN_HOST");
        sendMessage.putHeaders(MessageHeader.STORE_TIMESTAMP, new Date().getTime());
        sendMessage.putHeaders(MessageHeader.STORE_HOST, "SOME_STORE_HOST");
        sendMessage.putHeaders(MessageHeader.START_TIME, new Date().getTime());
        sendMessage.putHeaders(MessageHeader.STOP_TIME, new Date().getTime());
        sendMessage.putHeaders(MessageHeader.PRIORITY, 5);
        sendMessage.putHeaders(MessageHeader.RELIABILITY, 5);
        sendMessage.putHeaders(MessageHeader.SCHEDULE_EXPRESSION, "SOME_SCHEDULE_EXPRESSION");
        sendMessage.putHeaders(MessageHeader.SHARDING_KEY, "SOME_SHARDING_KEY");
        sendMessage.putHeaders(MessageHeader.SHARDING_PARTITION, "SOME_SHARDING_PARTITION");
        sendMessage.putHeaders(MessageHeader.TRACE_ID, "SOME_TRACE_ID");

        sendMessage.putProperties("property_string", "SOME_PROPERTY_STRING");
        sendMessage.putProperties("property_int", 3600);
        sendMessage.putProperties("property_double", 3.14D);
        sendMessage.putProperties("property_long", new Date().getTime());

        long start = System.currentTimeMillis();
        for (int i = 0; i < TIMES; i++) {
            sendMessage.putHeaders(MessageHeader.MESSAGE_ID, "" + i);
            producer.send(sendMessage);
        }
        long t1 = System.currentTimeMillis() - start;

        PullConsumer consumer1 = new DefaultPullConsumer(properties);
        consumer1.attachQueue(queue1, Collections.singletonList(topic1));

        start = System.currentTimeMillis();
        Message receiveMessage = null;
        for (int i = 0; i < TIMES; i++) {
            receiveMessage = consumer1.poll();
        }
        long t2 = System.currentTimeMillis() - start;
        System.out.printf("write cost:%5d,read cost:%5d,mps:%d\n", t1, t2, TIMES * 1000 / (t1 + t2));
        System.out.println(sendMessage.equals(receiveMessage));
    }
}
