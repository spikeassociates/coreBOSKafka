package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StreamConsumer extends Consumer {

    public StreamConsumer() throws Exception {
    }

    public void init() {

        List topics = new ArrayList();
        topics.add("second_stream_topic");
        kafkaConsumer.subscribe(topics);
        try {
            while (true) {
                ConsumerRecords records = kafkaConsumer.poll(10);
                Iterator it = records.iterator();
                while (it.hasNext()) {
                    ConsumerRecord record = (ConsumerRecord) it.next();
                    readData(record);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }

    public void readData(ConsumerRecord record) {

        System.out.println("Consumer Begin");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println(String.format("Topic - %s,Key - %s, Partition - %d, Value: %s", record.topic(), record.key(), record.partition(), record.value()));
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("Consumer End");
    }

    public static void main(String[] args) {
        try {
            new StreamConsumer().init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}