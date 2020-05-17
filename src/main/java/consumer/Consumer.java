package consumer;

import helper.Util;
import model.Modules;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import vtwslib.WSClient;

import java.util.Properties;

public class Consumer {

    protected static final String COREBOS_URL = Util.getProperty("corebos.consumer.url");
    protected static final String USERNAME = Util.getProperty("corebos.consumer.username");
    protected static final String ACCESS_KEY = Util.getProperty("corebos.consumer.access_key");
    protected static final String GROUP_ID = Util.getProperty("corebos.consumer.group_id");
    protected static final String KAFKA_URL = Util.getProperty("corebos.kafka.url");
    protected static final Modules modulesDeclared = Util.getObjectFromJson(Util.getProperty("corebos.consumer.modules"), Modules.class);

    protected static final String useFieldMapping = Util.getProperty("corebos.consumer.useFieldMapping");
    protected static final String fieldsDoQuery = Util.getProperty("corebos.consumer.fieldsDoQuery");

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
        properties.put("group.id", GROUP_ID);
//        properties.put("max.partition.fetch.bytes", 2097152);
        properties.put("max.poll.records", 100000);
//        properties.put("auto.commit.interval.ms", 8000);
        properties.put("enable.auto.commit", true);
        properties.put("request.timeout.ms", Integer.MAX_VALUE);
        properties.put("max.poll.interval.ms", Integer.MAX_VALUE);
        properties.put("session.timeout.ms", Integer.MAX_VALUE);
//        properties.put("heartbeat.interval.ms", Integer.MAX_VALUE);
//        properties.put("metadata_max_age_ms", 50000);
        kafkaConsumer = new KafkaConsumer(properties);
    }
}
