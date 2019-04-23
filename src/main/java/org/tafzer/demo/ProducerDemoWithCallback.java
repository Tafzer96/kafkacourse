package org.tafzer.demo;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args){

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        //  create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for(int i=0;i<10;i++ ){
            //create the Producer Record

            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world" + Integer.toString(i));


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
            });

        }


        //flush data
        producer.flush();

        //close data
        producer.close();
    }
}
