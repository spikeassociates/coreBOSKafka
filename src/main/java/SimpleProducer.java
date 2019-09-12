import Helper.Util;
import model.KeyData;
import model.ValueData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;

public class SimpleProducer {
    private static Producer<String, String> producer;
    private static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private static final String KAFKA_URL = Util.getProperty("corebos.kafka.url");

    public SimpleProducer() {
        Properties props = new Properties();
// Set the broker list for requesting metadata to find the lead broker
        props.put("metadata.broker.list", KAFKA_URL);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
//This specifies the serializer class for keys
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
// 1 means the producer receives an acknowledgment once the lead replica
// has received the data. This option provides better durability as the
// client waits until the server acknowledges the request as successful.
        props.put("request.required.acks", "1");
//        ProducerConfig config = new ProducerConfig(props);
//        producer = new Producer<String, String>(config);
        producer = new KafkaProducer(props);
    }

    public static void main(String[] args) {
//        testProducer();
        testProducer2();
        /*System.out.println("Starting");
        for (String arg : args)
            System.out.println("arg = " + arg);
//        args = new String[]{"first_topic", "1", "Ardit"};
        int argsCount = args.length;
        if (argsCount == 0 || argsCount == 1)
            throw new IllegalArgumentException("Please provide topic name and Message count as arguments");
// Topic name and the message count to be published is passed from the
// command line
        String topic = (String) args[0];
        String count = (String) args[1];
        String message = (String) args[2];
//        String topic = "first_topic";
//        String count = "1";
        int messageCount = Integer.parseInt(count);
        System.out.println("Topic Name - " + topic);
        System.out.println("Message Count - " + message);
        SimpleProducer simpleProducer = new SimpleProducer();
        simpleProducer.publishMessage(topic, messageCount, message);*/
    }


    private static void testProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        String topic = "first_topic";
        String key = "mykey";
        String value = "Ardit";
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);

        producer.send(producerRecord);

        producer.close();
    }

    private static void testProducer2() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        KeyData keyData = new KeyData();
        keyData.SquareId = "SquareID102554";
        ValueData valueData = new ValueData(keyData.SquareId);
        String topic = "first_topic";
        String key = Util.getJson(keyData);
        String value = Util.getJson(valueData);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);

        producer.send(producerRecord);

        producer.close();
    }

    private void publishMessage(String topic, int messageCount, String message) {
        for (int mCount = 0; mCount < messageCount; mCount++) {
            String runtime = new Date().toString();
            String msg = "Message Publishing Time - " + runtime + message;
            System.out.println(msg);
// Creates a KeyedMessage instance
// Publish the message
            producer.send(new ProducerRecord<String, String>(topic, msg));
        }
// Close producer connection with broker.
        producer.close();
    }
}
