package producer;

import helper.Util;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vtwslib.WSClient;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    protected static final String COREBOS_URL = Util.getProperty("corebos.producer.url");
    protected static final String USERNAME = Util.getProperty("corebos.producer.username");
    protected static final String ACCESS_KEY = Util.getProperty("corebos.producer.access_key");
    protected static final String MODULES = Util.getProperty("corebos.producer.modules");

    protected static org.apache.kafka.clients.producer.Producer<String, String> producer;
    protected static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    protected static final String KAFKA_URL = Util.getProperty("corebos.kafka.url");

    protected static final String DEFAULT_KEY = "DEFAULT KEY";
    protected static final String DEFAULT_VALUE = "DEFAULT VALUE";
    protected Map moduleMap = new HashMap();



    protected WSClient wsClient;

    public Producer() throws Exception {
        wsClient = new WSClient(COREBOS_URL);
        if (!wsClient.doLogin(USERNAME, ACCESS_KEY)) {
            throw new Exception("Login error");
        }

        Object[] modules = wsClient.doDescribe(MODULES).values().toArray();
        for (Object module : modules) {
            Map theMap = (Map) module;
            moduleMap.put(theMap.get("idPrefix"), theMap);
        }

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
//        producer = new KafkaProducer(props, new StringSerializer(),
//                new KafkaJsonSerializer());
        producer = new KafkaProducer(props);
    }

    protected void publishMessage(String topic, String key, String message) {
        String runtime = new Date().toString();
        String msg = "Message Publishing Time - " + runtime + message;
        System.out.println(msg);
// Creates a KeyedMessage instance
// Publish the message
        try {
            RecordMetadata metadata = producer.send(new ProducerRecord<String, String>(topic, key, message)).get();
            System.out.printf("Record sent with key %s to partition %d with offset " + metadata.offset() + " with value %s Time %s"
                    , key, metadata.partition(), message, runtime);
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
// Close producer connection with broker.
        // producer.close();
    }

}
