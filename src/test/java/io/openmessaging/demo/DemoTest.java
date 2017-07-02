package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;

import java.io.IOException;
import java.util.Collections;

/**
 * Created by Zhang JinLong(150429) on 2017-06-28.
 */
public class DemoTest {

    public static void main(String[] args) throws IOException {
        KeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "/home/closer/om");
        Producer producer = new DefaultProducer(properties);
        String queue1 = "QUEUE1";
        String topic1 = "TOPIC1";
        Message sendMessage = producer.createBytesMessageToQueue(topic1, topic1.getBytes());
        producer.send(sendMessage);
        //请保证数据写入磁盘中
        producer.flush();

        producer.send(sendMessage);
        //请保证数据写入磁盘中
        producer.flush();

        PullConsumer consumer1 = new DefaultPullConsumer(properties);
        consumer1.attachQueue(queue1, Collections.singletonList(topic1));

        Message receiveMessage = consumer1.poll();
        System.out.println(receiveMessage.toString());

        receiveMessage = consumer1.poll();
        System.out.println(receiveMessage.toString());
    }
}
