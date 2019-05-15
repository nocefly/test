package com.nocefly.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Title: ProducerThread
 * @Package: com.nocefly.kafka
 * @Description: TODO（添加描述）
 * @Author: Wujiong
 * @Data: 2018/12/27
 * @Version: V1.0
 */
public class ProducerThread implements Runnable{
    private Properties properties;
    public ProducerThread(){
        properties = new Properties();
        properties.put("bootstrap.servers", "192.168.184.133:19092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    }

    public void run() {
        try( Producer<String, String> producer = new KafkaProducer<String, String>(properties)){
            for (int i = 0; i < 10; i++) {
                String msg = "Message " + i;
                producer.send(new ProducerRecord<>("HelloWorld", msg));
                System.out.println("Sent:" + msg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
