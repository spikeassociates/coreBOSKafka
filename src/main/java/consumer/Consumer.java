package consumer;

import helper.Util;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import vtwslib.WSClient;

import java.util.Properties;

public class Consumer {

    protected static final String COREBOS_URL = Util.getProperty("corebos.test.url");
    protected static final String USERNAME = Util.getProperty("corebos.username");
    protected static final String ACCESS_KEY = Util.getProperty("corebos.access_key");
    protected static final String KAFKA_URL = Util.getProperty("corebos.kafka.url");
    protected static final String MODULES = Util.getProperty("corebos.modules");

    protected Properties properties = new Properties();
    protected KafkaConsumer kafkaConsumer;
    protected WSClient wsClient;


    public Consumer() throws Exception {
        wsClient = new WSClient(COREBOS_URL);
        if (!wsClient.doLogin(USERNAME, ACCESS_KEY)) {
            throw new Exception("Login error");
        }
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");
        kafkaConsumer = new KafkaConsumer(properties);
    }
}
