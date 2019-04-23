package org.tafzer.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();

    }
    private ConsumerDemoWithThread(){

    }
    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread .class.getName());

        String GroupID = "Tafzer_Groupe";
        String topic = "first_topic";
        String bootstrapServer = "127.0.0.1:9092";
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer Thread");
        //create the consumer rUNNABLE
        Runnable myConsumerThread = new ConsumerThread(
                topic,
                GroupID,
                bootstrapServer,
                latch);
        Thread thread = new Thread(myConsumerThread);
        thread.start();

        //Add a shutdiwn hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    logger.info("caught shutdown hook");
                    ((ConsumerThread) myConsumerThread).shutdown();
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    logger.info("Application closed ");
                }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable{
        private Logger logger = LoggerFactory.getLogger(ConsumerThread .class.getName());
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        public ConsumerThread(String topic, String GroupID , String bootstrapServer, CountDownLatch latch) {
            this.latch = latch;

            //Create consumer Config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GroupID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


            //create Consumer

            consumer= new KafkaConsumer<String, String>(properties);

            //suscribe Consumer to our topic
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String, String> record : records){
                        logger.info(
                                "Key :" + record.key() + "\n" +
                                        "Value : " + record.value() + "\n" +
                                        "Parition : " + record.partition() + "\n" +
                                        "Offset : "+ record.offset() + "\n") ;
                    }
                }
            }catch (WakeupException e){
                 logger.info("Received shutdown signal");
            }finally {
                consumer.close();
                latch.countDown();
            }

        }
        public void shutdown(){
            //the wakeup is a special method to interupp consumer.poll
            consumer.wakeup();
        }

    }
}
