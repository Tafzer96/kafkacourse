package org.tafzer.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKey {
    public static void main(String[] args) throws ExecutionException, InterruptedException{

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        //  create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for(int i=0;i<10;i++ ){
            String topic = "first_topic";
            String value = "hello world"+ i;
            String key = "id_"+ i;
            //create the Producer Record

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key : "+ key);//log the key
            //id_0 is going to parition 1
            //id_1 is going to parition 0
            //id_2 is going to parition 2


            // Send Data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //execute every time a record is succesfully sent or an exception is thrown
                    if(e == null){
                        //the record was succesfully sent
                        logger.info("Receive new MetaData. \n" +
                                "topic :" + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "TimesTamp : "+ recordMetadata.timestamp() + "\n") ;
                    }
                    else{
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //block the .send() to make it synchronous - don't do this in production !

        }


        //flush data
        producer.flush();

        //close data
        producer.close();
    }
}
