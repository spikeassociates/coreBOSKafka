package no_corebos;

import helper.Log;
import helper.Util;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class Consumer {

    protected static final String GROUP_ID = Util.getProperty("consumer.group");
    protected static final String KAFKA_URL = Util.getProperty("consumer.kafka.url");
    protected static final String TOPICS = Util.getProperty("consumer.topic");

    protected Properties properties = new Properties();
    protected KafkaConsumer kafkaConsumer;


    public Consumer() {
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", GROUP_ID);
//        properties.put("request.timeout.ms", "test-group");
        kafkaConsumer = new KafkaConsumer(properties);
    }

    public void init() {
        try {
            List topics = Arrays.asList(TOPICS.replace(" ", "").split(","));
            kafkaConsumer.subscribe(topics);
            while (true) {
                ConsumerRecords records = kafkaConsumer.poll(10);
                Iterator it = records.iterator();
                while (it.hasNext()) {
                    ConsumerRecord record = (ConsumerRecord) it.next();
                    Log.getLogger().info(String.format("Topic - %s, Partition - %d, Key: %s, Value: %s", record.topic(), record.partition(), record.key(), record.value()));
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }
}
