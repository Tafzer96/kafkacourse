package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSick {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroup .class.getName());
        Properties properties = new Properties();
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";

        //Create consumer Config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        //create Consumer
        KafkaConsumer<String, String> consumer= new KafkaConsumer<String, String>(properties);

        //Assign and seek  are mostly use to replay  data or fetch a specific message

        //assign
        TopicPartition partition =new TopicPartition(topic, 0);
        long OffsetToreadFrom = 10L;
        consumer.assign(Arrays.asList(partition));

        //seek

        consumer.seek(partition, OffsetToreadFrom);


        int nbMessageToRead = 4;
        boolean keepread = true;
        int nbReadTrue = 0;

        //poll of the new data

        while(keepread){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records){
                nbReadTrue++;
                logger.info("Key : " + record.key() + "\n" +
                        "Value : " + record.value() + "\n" +
                        "Parition : " + record.partition() + "\n" +
                        "Offset : "+ record.offset() + "\n") ;

                if(nbReadTrue >= nbMessageToRead){
                    keepread = false;
                    break;
                }

            }
        }
        logger.info("Exit the Application");

    }
}
