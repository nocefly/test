package com.nocefly.kafka;

/**
 * @Title: App
 * @Package: com.nocefly.kafka
 * @Description: TODO（添加描述）
 * @Author: Wujiong
 * @Data: 2018/12/27
 * @Version: V1.0
 */
public class App {
    /**
     * 注册hook程序，保证线程能够完整执行
     *
     * @param checkThread
     */
    private static void addShutdownHook(final Thread checkThread) {
        //为了保证TaskThread不在中途退出，添加ShutdownHook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("收到关闭信号，hook起动，开始检测线程状态 ...");
                //不断检测一次执行状态，如果线程一直没有执行完毕，超时后，放弃等待       \
                for (int i = 0; i < 100; i++) {
                    if (checkThread.getState() == State.TERMINATED) {
                        System.out.println("检测到线程执行完毕，退出hook");
                        return;
                    }
                    App.sleep(100);
                }
                System.out.println("检测超时，放弃等待，退出hook，此时线程会被强制关闭");
            }
        });
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args){
        Thread consumer = new Thread(new ConsumerThread());
        Thread producer = new Thread(new ProducerThread());
        consumer.start();
        producer.start();
    }
}
