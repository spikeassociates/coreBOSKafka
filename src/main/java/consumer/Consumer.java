package consumer;

import helper.Util;
import model.Modules;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import vtwslib.WSClient;

import java.util.Properties;
@SuppressWarnings("ALL")
public class Consumer {

    protected static final String COREBOS_URL = (Util.getProperty("consumer.corebos.webservice.url").isEmpty()) ?
            System.getenv("COREBOS_BASE_URL") : Util.getProperty("consumer.corebos.webservice.url");
    protected static final String USERNAME = (Util.getProperty("consumer.corebos.username").isEmpty()) ?
            System.getenv("COREBOS_USER") : Util.getProperty("consumer.corebos.username");
    protected static final String ACCESS_KEY = (Util.getProperty("consumer.corebos.accesskey").isEmpty()) ?
            System.getenv("COREBOS_ACCESSKEY") : Util.getProperty("consumer.corebos.accesskey");
    protected static final String GROUP_ID = (Util.getProperty("consumer.groupid").isEmpty()) ?
            System.getenv("CONSUMER_GROUP_ID") : Util.getProperty("consumer.groupid");
    protected static final String KAFKA_URL = (Util.getProperty("kafka.url").isEmpty()) ?
            System.getenv("KAFKA_HOST") : Util.getProperty("kafka.url");

    private String securityProtocol = (Util.getProperty("kafka.security.protocol").isEmpty()) ?
            System.getenv("KAFKA_SECURITY_PROTOCOL") : Util.getProperty("kafka.security.protocol");
    private String saslMechanism = (Util.getProperty("kafka.sasl.mechanism").isEmpty()) ?
            System.getenv("KAFKA_SASL_MECHANISM") : Util.getProperty("kafka.sasl.mechanism");
    private String sasljaasconfig =  (Util.getProperty("kafka.sasl.jaas.config").isEmpty()) ?
            System.getenv("KAFKA_SASL_JAAS_CONFIG") : Util.getProperty("kafka.sasl.jaas.config");

    protected Properties properties = new Properties();
    protected KafkaConsumer kafkaConsumer;
    protected WSClient wsClient;


    public Consumer() throws Exception {
        wsClient = new WSClient(COREBOS_URL);
        if (!wsClient.doLogin(USERNAME, ACCESS_KEY)) {
            throw new Exception("Login error");
        }
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        properties.put("security.protocol", securityProtocol);
        properties.put("sasl.mechanism", saslMechanism);
        properties.put("sasl.jaas.config", sasljaasconfig);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", GROUP_ID);
        properties.put("max.poll.records", 5);
        properties.put("enable.auto.commit", false);
        properties.put("max.poll.interval.ms", Integer.MAX_VALUE);
        properties.put("session.timeout.ms", 125000);
        properties.put("heartbeat.interval.ms", 120000);
        kafkaConsumer = new KafkaConsumer(properties);
    }
}
