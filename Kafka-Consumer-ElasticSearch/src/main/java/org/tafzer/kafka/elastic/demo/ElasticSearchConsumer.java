package org.tafzer.kafka.elastic.demo;


import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static KafkaConsumer<String, String> createConsumer(String topic){
        Properties properties = new Properties();

        String GroupID = "Kadka-ElasticSearch";

        //Create consumer Config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GroupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        //create Consumer
        KafkaConsumer<String, String> consumer= new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
    public static void main(String[] args) throws IOException{
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

        //IndexRequest indexRequest = new IndexRequest(
          //      "twitter",
           //     "tweet"
        //);
        //String jsonString = "{" +
          //      "\"foo\":\"bar\"" +
            //    "}";
        //indexRequest.source(jsonString, XContentType.JSON);



        KafkaConsumer<String, String> consumer = createConsumer("topic_tweet");

        //poll of the new data

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records){

                IndexRequest indexRequest = new IndexRequest(
                        "twitter",
                        "tweet"
                );
                String jsonString = record.value();
                indexRequest.source(jsonString, XContentType.JSON);

                IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
                String id = response.getId();
                logger.info(id);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }

        //close the client
        //client.close();

    }
}
