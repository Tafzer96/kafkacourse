package org.tafzer.kafka.elastic.demo;


import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable auto commit of Offset
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");


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



        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweet");

        //poll of the new data

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            Integer recordCount = records.count();

            logger.info("Received" + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();

            for(ConsumerRecord<String, String> record : records){

                //Kafka Generic Idea
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                //twitter feed specific id

                try {
                    String id = extractIdFromtweet(record.value());

                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweet",
                            id
                    ).source(record.value(), XContentType.JSON);
                    bulkRequest.add(indexRequest); // we add to our bulkRequest (takes no time)
                }catch (NullPointerException e){
                    logger.warn("Skipping Bad Data" + record.value()) ;
                }


            }
            if(recordCount > 0){
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("committing the offset");
                consumer.commitSync();
                logger.info("offset has been committed");

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
    private static JsonParser jsonParser = new JsonParser();

    public static String extractIdFromtweet(String tweetJson){
        return jsonParser.parse(tweetJson).
                getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
