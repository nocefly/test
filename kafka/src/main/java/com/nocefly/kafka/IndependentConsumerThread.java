package com.nocefly.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Title: ConsumerThread
 * @Package: com.nocefly.kafka
 * @Description: TODO（添加描述）
 * @Author: Wujiong
 * @Data: 2018/12/27
 * @Version: V1.0
 */
public class IndependentConsumerThread implements Runnable{
    private Properties properties;
    private String topic = "HelloWorld";

    public IndependentConsumerThread(){
        properties = new Properties();
        properties.put("bootstrap.servers", "192.168.184.133:19092");
        //properties.put("group.id", "group-1");
        properties.put("enable.auto.commit", "false");
        //properties.put("auto.commit.interval.ms", "1000");
        //properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    @Override
    public void run() {

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
        List<TopicPartition> partitions = new ArrayList<>();
        if(partitionInfos!=null){
            for(PartitionInfo info:partitionInfos){
                partitions.add(new TopicPartition(info.topic(),info.partition()));
            }
            kafkaConsumer.assign(partitions);
        }

       try{
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                    System.out.println();
                    kafkaConsumer.commitAsync();
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            System.out.println("commit when ends..");
            try{
                kafkaConsumer.commitSync();
            }
            finally {
                kafkaConsumer.close();
            }
        }
    }
}
