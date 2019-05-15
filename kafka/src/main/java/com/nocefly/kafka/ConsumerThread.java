package com.nocefly.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * @Title: ConsumerThread
 * @Package: com.nocefly.kafka
 * @Description: TODO（添加描述）
 * @Author: Wujiong
 * @Data: 2018/12/27
 * @Version: V1.0
 */
public class ConsumerThread implements Runnable{
    private Properties properties;

    public ConsumerThread(){
        properties = new Properties();
        properties.put("bootstrap.servers", "192.168.184.133:19092");
        properties.put("group.id", "group-1");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    private class RebalanceHandle implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            System.out.println("on partition revoked event ...");

        }
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            System.out.println("on partition assigned event ...");
        }
    }

    @Override
    public void run() {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList("HelloWorld"),new RebalanceHandle());

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("consumer offset = %d, value = %s", record.offset(), record.value());
                System.out.println();
            }
            if(records.isEmpty()==false){
                System.out.println("begin to sleep.");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("end to sleep.");
            }
        }
    }
}
