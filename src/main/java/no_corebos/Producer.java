package no_corebos;

import helper.Log;
import helper.Util;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import producer.SimpleProducer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {


    protected static org.apache.kafka.clients.producer.Producer<String, String> producer;
    protected static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private static final String KAFKA_URL = Util.getProperty("producer.kafka.url");
    private static final String DEFAULT_TOPIC = Util.getProperty("producer.topic");
    private static final String DEFAULT_KEY = Util.getProperty("producer.key");
    private static final String DEFAULT_VALUE = Util.getProperty("producer.value");


    public Producer() {

        Properties props = new Properties();
        props.put("metadata.broker.list", KAFKA_URL);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("request.required.acks", "1");
        producer = new KafkaProducer(props);
    }

    public void publishMessage(String topic, String key, String message) {
        try {
            if (topic == null)
                topic = DEFAULT_TOPIC;
            if (key == null)
                key = DEFAULT_KEY;
            if (message == null)
                message = DEFAULT_VALUE;
            RecordMetadata metadata = producer.send(new ProducerRecord<String, String>(topic, key, message)).get();
            Log.getLogger().info("topic = " + topic);
            Log.getLogger().info("key = " + key);
            Log.getLogger().info("message = " + message);
            Log.getLogger().info("metadata.partition() = " + metadata.partition());
        } catch (InterruptedException e) {
            Log.getLogger().error(e.getMessage());
        } catch (ExecutionException e) {
            Log.getLogger().error(e.getMessage());
        }
// Close producer connection with broker.

    }

    public void close() {
        producer.close();
    }
}
