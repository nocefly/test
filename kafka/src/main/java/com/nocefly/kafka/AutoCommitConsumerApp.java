package com.nocefly.kafka;

/**
 * @Title: App
 * @Package: com.nocefly.kafka
 * @Description: TODO（添加描述）
 * @Author: Wujiong
 * @Data: 2018/12/27
 * @Version: V1.0
 */
public class AutoCommitConsumerApp {
    public static void main(String[] args){
        Thread consumer = new Thread(new AutoCommitConsumerThread());
        consumer.start();
    }
}
