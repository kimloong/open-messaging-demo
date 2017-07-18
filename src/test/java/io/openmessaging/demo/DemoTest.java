package io.openmessaging.demo;

import io.openmessaging.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;

/**
 * Created by Zhang JinLong(150429) on 2017-06-28.
 */
public class DemoTest {

    private static final int TIMES = 1_000_000;

    public static void main(String[] args) throws IOException {
        KeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "/home/closer/om");
        Producer producer = new DefaultProducer(properties);
        String queue1 = "QUEUE1";
        String topic1 = "TOPIC1";
        Message sendMessage = producer.createBytesMessageToQueue(queue1, new byte[4 * 1024]);

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

        for (int i = 0; i < TIMES; i++) {
            producer.send(sendMessage);
        }
        //请保证数据写入磁盘中
        producer.flush();

        PullConsumer consumer1 = new DefaultPullConsumer(properties);
        consumer1.attachQueue(queue1, Collections.singletonList(topic1));

        Message receiveMessage = null;
        for (int i = 0; i < TIMES; i++) {
            receiveMessage = consumer1.poll();
        }
        System.out.println(sendMessage.equals(receiveMessage));
    }
}
