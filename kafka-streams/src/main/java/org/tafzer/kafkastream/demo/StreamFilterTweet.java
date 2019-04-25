package org.tafzer.kafkastream.demo;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamFilterTweet {
    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-stream");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());


        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String, String> inpuTopic = streamsBuilder.stream("twitter_topics");
        KStream<String, String> filteredStream = inpuTopic.filter(
                (k,jsonTweet) -> extractUserFollowerIntweets(jsonTweet) > 10000
        );

        filteredStream.to("important_tweets");
        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties);

        //starts our stream application
        kafkaStreams.start();
    }

    private static JsonParser jsonParser = new JsonParser();

    public static Integer extractUserFollowerIntweets(String tweetJson){

         try {
             return jsonParser.parse(tweetJson).
                     getAsJsonObject()
                     .get("user")
                     .getAsJsonObject()
                     .get("followers_count")
                     .getAsInt();
         }
         catch (NullPointerException e){
             return 0;
         }
    }

}
