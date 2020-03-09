package kafka;

import helper.Util;

import java.util.Properties;

public class KafkaConfig {
    static final String COREBOS_URL = Util.getProperty("corebos.siae.url");
    static final String GROUP_ID = Util.getProperty("corebos.siae.group_id");
    static final String KAFKA_URL = Util.getProperty("corebos.kafka.url");
    static final String save_topic = Util.getProperty("corebos.siae.save_topic");
    static final String update_topic = Util.getProperty("corebos.siae.update_topic");
    static final String signed_topic = Util.getProperty("corebos.siae.signed_topic");
    static final String get_topic = Util.getProperty("corebos.siae.get_topic");
    static final String notify_topic = Util.getProperty("corebos.siae.notify_topic");
    static final String error_topic = Util.getProperty("corebos.siae.error_topic");

    Properties properties = new Properties();
}
