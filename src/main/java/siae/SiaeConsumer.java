package siae;

import helper.Log;
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
                    Log.getLogger().info(String.format("Topic - %s, Key - %s, Partition - %d, Value: %s", record.topic(), record.key(), record.partition(), record.value()));
                    updateWsClient(record);
                    updateRecord(record);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Log.getLogger().error(e.getMessage());
        } finally {
            System.out.println("Closing Siae Producer And Consumer");
            Log.getLogger().info("Closing Siae Producer And Consumer");
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
        } else if (record.topic().equals(save_topic) || record.topic().equals(signed_topic)) {
            Object value = Util.getObjectFromJson((String) record.value(), Object.class);
            createRecord((Map) value, keyData);
        } else if (record.topic().equals(update_topic)) {
            Object value = Util.getObjectFromJson((String) record.value(), Object.class);
            updateRecord((Map) value, keyData);
        }


    }

    private void getCBModule(SiaeKeyData keyData) {
        Map error = new HashMap();
        error.put("key", keyData);
        if (keyData.module != null && !keyData.module.equals("") &&
                keyData.nameOrIdFieldName != null && !keyData.nameOrIdFieldName.equals("")) {
            error.put("value", "Miss module field");
            producer.publishMessage(error_topic, null, Util.getJson(error));
            return;
        }
        String module = keyData.module;
        if (module.equals("orderTickets") || module.equals("orderAbbonamenti"))
            module = "cbElencoTitoli";

        String query;
        if (keyData.nameOrId != null && !keyData.nameOrId.equals("") && keyData.date != null && !keyData.date.equals("")) {
            query = "select * from " + module +
                    " where (id = '" + keyData.nameOrId + "' or " + keyData.nameOrIdFieldName + " = '" + keyData.nameOrId + "') " +
                    "and createdtime = '" + keyData.date + "';";
        } else if (keyData.nameOrId != null && !keyData.nameOrId.equals("")) {
            query = "select * from " + module +
                    " where (id = '" + keyData.nameOrId + "' or " + keyData.nameOrIdFieldName + " = '" + keyData.nameOrId + "');";
        } else {
            error.put("value", "Miss nameOrId field");
            producer.publishMessage(error_topic, null, Util.getJson(error));
            return;
        }
        System.out.println("query = " + query);
        Log.getLogger().info("query = " + query);
        Object res = wsClient.doQuery(query);
        if (res == null) {
            error.put("value", "Error on: " + query);
            producer.publishMessage(error_topic, null, Util.getJson(error));
            return;
        }
        System.out.println("Response size = " + ((List) res).size());
        Log.getLogger().info("Response size = " + ((List) res).size());
        for (Object m : (List) res) {
            producer.publishMessage(notify_topic, Util.getJson(keyData), Util.getJson(m));
        }

    }

    private void updateRecord(Map element, SiaeKeyData keyData) {
        String method = Util.methodUPDATE;

        generateMapToSend(element, keyData, method);
    }


    private void createRecord(Map element, SiaeKeyData keyData) {
        String method = Util.methodCREATE;
        if (keyData.module.equals("cbManifestazioni")) {
            method = "createManifestation";
            if (element.get("logomanifest") != null) {
                Log.getLogger().info("Reformat logomanifest value");
                String formated = ((String) element.get("logomanifest")).replace("/", "\\/");
                element.put("logomanifest", formated);
            }
        } else if (keyData.module.equals("cbAbbonamenti"))
            method = "createAbbonamento";
        else if (keyData.module.equals("orderTickets") || keyData.module.equals("orderAbbonamenti"))
            method = "createTitoli";

        generateMapToSend(element, keyData, method);
    }

    private void generateMapToSend(Map element, SiaeKeyData keyData, String method) {
        Map<String, Object> mapToSend = new HashMap<>();
        String module = keyData.module;
        if (keyData.module.equals("orderTickets"))
            module = "Ticket";
        else if (keyData.module.equals("orderAbbonamenti"))
            module = "Abbonamento";

        element.put("assigned_user_id", wsClient.getUserID());
        element.put("element_type", module);

        mapToSend.put("elementType", module);
        mapToSend.put("element", Util.getJson(element));

        Object moduleData = wsClient.doInvoke(method, mapToSend, "POST");
        System.out.println("Util.getJson(d) = " + Util.getJson(moduleData));
        Log.getLogger().info("Util.getJson(d) = " + Util.getJson(moduleData));
        if (moduleData != null) {
            producer.publishMessage(notify_topic, Util.getJson(keyData), Util.getJson(moduleData));
            if (keyData.module.equals("orderTickets") || (keyData.module.equals("orderAbbonamenti")))
                producer.publishMessage("ticket_access_information", (String) ((Map) moduleData).get("barcode"), Util.getJson(moduleData));
        } else {
            Map error = new HashMap();
            error.put("key", keyData);
            error.put("value", element);
            producer.publishMessage(error_topic, null, Util.getJson(error));
        }
    }

}