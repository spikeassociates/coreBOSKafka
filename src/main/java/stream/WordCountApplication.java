package stream;

import helper.Util;
import model.ValueData;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApplication {

    public static void main(final String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Stream.KAFKA_URL);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
//        KStream<String, String> textLines = builder.stream("TextLinesTopic");
        KStream<String, String> textLines = builder.stream(Stream.firstStreamTopic);
        KTable<String, Long> wordCounts = textLines
//                .flatMapValues(val -> Arrays.asList(val.toLowerCase().split("\\W+")))
//                .groupBy((key, word) -> word)
                .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                    @Override
                    public Iterable<String> apply(String textLine) {
                        System.out.println("textLine = " + textLine);
                        ValueData value = Util.getObjectFromJson(textLine, ValueData.class);
//                        value.user.firstName = "John";
//                        value.user.lastName = "Doe";
                        return Arrays.asList(Util.getJson(value));
//                        return Arrays.asList(textLine.split("\\W+"));
                    }
                })
                .groupBy(new KeyValueMapper<String, String, String>() {
                    @Override
                    public String apply(String key, String word) {
                        System.out.println("key = " + key);
                        System.out.println("word = " + word);
                        return word;
                    }
                })
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));


        wordCounts.toStream().to(Stream.secondStreamTopic, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

}