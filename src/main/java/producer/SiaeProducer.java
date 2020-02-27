package producer;

import helper.Util;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SiaeProducer {


    protected static org.apache.kafka.clients.producer.Producer<String, String> producer;
    protected static final Logger logger = LoggerFactory.getLogger(SiaeProducer.class);
    protected static final String KAFKA_URL = Util.getProperty("corebos.kafka.url");


    public SiaeProducer() {
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
        String runtime = new Date().toString();
        String msg = "Message Publishing Time - " + runtime + message;
        System.out.println(msg);
            try {
                RecordMetadata metadata = producer.send(new ProducerRecord<String, String>(topic,  key, message)).get();
                System.out.println("topic = " + topic);
                System.out.println("key = " + key);
                System.out.println("message = " + message);
                System.out.println("metadata.partition() = " + metadata.partition());
                System.out.println(msg);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
    }

    public void close() {
        producer.close();
    }
}
