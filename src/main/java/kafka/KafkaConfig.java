package kafka;

import helper.Util;
import model.Modules;
import vtwslib.WSClient;

import java.util.Properties;

public class KafkaConfig {
    static final String COREBOS_URL = Util.getProperty("corebos.consumer.url");
    static final String USERNAME = Util.getProperty("corebos.consumer.username");
    static final String ACCESS_KEY = Util.getProperty("corebos.consumer.access_key");
    static final String GROUP_ID = Util.getProperty("corebos.consumer.group_id");
    static final String KAFKA_URL = Util.getProperty("corebos.kafka.url");
    static final String save_topic = Util.getProperty("corebos.siae.save_topic");
    static final String update_topic = Util.getProperty("corebos.siae.update_topic");
    static final String signed_topic = Util.getProperty("corebos.siae.signed_topic");
    static final String get_topic = Util.getProperty("corebos.siae.get_topic");
    static final String notify_topic = Util.getProperty("corebos.siae.notify_topic");

    Properties properties = new Properties();
}
