package siae;

import helper.Util;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import vtwslib.WSClient;

import java.util.*;

public class SiaeConsumer extends KafkaConfig {

    private KafkaConsumer kafkaConsumer;

    private WSClient wsClient = new WSClient(COREBOS_URL);

    private SiaeProducer producer;

    public SiaeConsumer() {

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", GROUP_ID);
//        properties.put("request.timeout.ms", "test-group");
        kafkaConsumer = new KafkaConsumer(properties);

        producer = new SiaeProducer();
        List topics = new ArrayList();
        topics.add(save_topic);
        topics.add(update_topic);
        topics.add(signed_topic);
        topics.add(get_topic);

        kafkaConsumer.subscribe(topics);
    }

    public void init() {

        try {
            while (true) {
                ConsumerRecords records = kafkaConsumer.poll(10);
                Iterator it = records.iterator();
                while (it.hasNext()) {
                    ConsumerRecord record = (ConsumerRecord) it.next();
                    System.out.println(String.format("Topic - %s, Key - %s, Partition - %d, Value: %s", record.topic(), record.key(), record.partition(), record.value()));
                    updateWsClient(record);
                    updateRecord(record);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            System.out.println("Closing Siae Producer And Consumer");
            producer.close();
            kafkaConsumer.close();
        }
    }

    private void updateWsClient(ConsumerRecord record) {
        SiaeKeyData keyData = Util.getObjectFromJson((String) record.key(), SiaeKeyData.class);
        wsClient.set_userid(keyData.userID);
        wsClient.set_sessionid(keyData.sessionID);
    }

    private void updateRecord(ConsumerRecord record) throws Exception {
        SiaeKeyData keyData = Util.getObjectFromJson((String) record.key(), SiaeKeyData.class);
        if (record.topic().equals(get_topic)) {
            getCBModule(keyData);
        } else if (record.topic().equals(save_topic)) {
            Object value = Util.getObjectFromJson((String) record.value(), Object.class);
            createRecord((Map) value, keyData);
        } else if (record.topic().equals(update_topic)) {
            Object value = Util.getObjectFromJson((String) record.value(), Object.class);
            updateRecord((Map) value, keyData);
        }


    }

    private void getCBModule(SiaeKeyData keyData) {
//        Object response = wsClient.doRetrieve();

        producer.publishMessage(notify_topic, Util.getJson(keyData), "the message GEt");
    }

    private void updateRecord(Map element, SiaeKeyData keyData) {
        String method = Util.methodUPDATE;

        generateMapToSend(element, keyData, method);
    }


    private void createRecord(Map element, SiaeKeyData keyData) {
        String method = Util.methodCREATE;
        if (keyData.module.equals("cbManifestazioni"))
            method = "createManifestation";
        else if (keyData.module.equals("cbAbbonamenti"))
            method = "createAbbonamento";
        else if (keyData.module.equals("orderTickets") || keyData.module.equals("orderAbbonamenti"))
            method = "createTitoli";

        generateMapToSend(element, keyData, method);
    }

    private void generateMapToSend(Map element, SiaeKeyData keyData, String method) {
        Map<String, Object> mapToSend = new HashMap<>();

        element.put("assigned_user_id", wsClient.getUserID());

        mapToSend.put("elementType", keyData.module);
        mapToSend.put("element", Util.getJson(element));

        Object moduleData = wsClient.doInvoke(method, mapToSend, "POST");
        System.out.println("Util.getJson(d) = " + Util.getJson(moduleData));
        if (moduleData != null) {
            producer.publishMessage(notify_topic, Util.getJson(keyData), Util.getJson(moduleData));
        } else {
            producer.publishMessage(error_topic, Util.getJson(keyData), Util.getJson(mapToSend));
        }
    }

}