package tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author viveksoni100
 */
public class StreamsFilterTweets {

    public static Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class.getName());

    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {

        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-stream");   // similar to kafka-consumer-groups
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets"); // this is going to be topic we read from
        KStream<String, String> filteredStream = inputTopic.filter((k, jsonTweet) ->
            // filter for tweets which has a user of over 5000 followers
            extractUserFollowersInTweets(jsonTweet) > 5000
        );
        filteredStream.to("important_tweets");

        // build a topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // starts our stream application
        kafkaStreams.start();
    }

    private static Integer extractUserFollowersInTweets(String tweetJson) {
        // gson library
        try {
            return jsonParser.parse(tweetJson).getAsJsonObject()
                    .get("user").getAsJsonObject()
                    .get("followers_count").getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }
}
