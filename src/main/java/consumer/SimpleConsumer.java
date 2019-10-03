package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class SimpleConsumer extends Consumer {

    public SimpleConsumer() throws Exception {
    }

    public void init() {

        List topics = new ArrayList();
        topics.add("first_topic");
        kafkaConsumer.subscribe(topics);
        try {
            while (true) {
                ConsumerRecords records = kafkaConsumer.poll(10);
                Iterator it = records.iterator();
                while (it.hasNext()) {
                    ConsumerRecord record = (ConsumerRecord) it.next();
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));
                    System.out.println(record.toString());
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }
}