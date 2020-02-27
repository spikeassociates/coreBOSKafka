package consumer;

import helper.Util;
import model.KeyData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.*;

public class UpdateConsumer extends Consumer {

    private final String topic = Util.getProperty("corebos.consumer.topic");

    public UpdateConsumer() throws Exception {
        List topics = new ArrayList();
        topics.add(topic);
        kafkaConsumer.subscribe(topics);
    }

    public void init() {

        try {
            while (true) {
                ConsumerRecords records = kafkaConsumer.poll(10);
                Iterator it = records.iterator();
                while (it.hasNext()) {
                    ConsumerRecord record = (ConsumerRecord) it.next();
                    readRecord(record);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }


    private void readRecord(ConsumerRecord record) {
        System.out.println(String.format("Topic - %s, Key - %s, Partition - %d, Value: %s", record.topic(), record.key(), record.partition(), record.value()));
        KeyData keyData = Util.getObjectFromJson((String) record.key(), KeyData.class);
        Object value = Util.getObjectFromJson((String) record.value(), Object.class);

        if (keyData.operation.equals(Util.methodUPDATE)) {
            System.out.println("Upserting the Record");
            upsertRecord(keyData.module, (Map) value);
        } else if (keyData.operation.equals(Util.methodDELETE)) {
            System.out.println("Deleting the Record");
            deleteRecord(keyData.module, (String) value);
        }
    }

    private void upsertRecord(String module, Map element) {
        String modulesIdField = modulesDeclared.getFieldsDoQuery(module).get(0);
        Map<String, Object> mapToSend = new HashMap<>();
        Map<String, Object> fieldUpdate = new HashMap<>();

        for (String filedl : modulesDeclared.getFieldsConsiderate(module))
            fieldUpdate.put(filedl, element.get(filedl));
        fieldUpdate.put("assigned_user_id", wsClient.getUserID());
        fieldUpdate.put(modulesIdField, element.get("id"));

        mapToSend.put("elementType", module);
        mapToSend.put("element", Util.getJson(fieldUpdate));
        mapToSend.put("searchOn", modulesIdField);

        Object d = wsClient.doInvoke(Util.methodUPSERT, mapToSend, "POST");
        System.out.println("Util.getJson(d) = " + Util.getJson(d));

    }

    private void deleteRecord(String module, String value) {
        Object response = getRecord(module, value);
        if (response == null || ((List) response).size() == 0) {
            return;
        }
        Map element = ((Map) ((List) response).get(0));
        Map<String, Object> mapToSend = new HashMap<>();

        mapToSend.put("elementType", module);
        mapToSend.put("id", element.get("id"));

        Object d = wsClient.doInvoke(Util.methodDELETE, mapToSend, "POST");
        System.out.println("Util.getJson(d) = " + Util.getJson(d));
    }

    private Object getRecord(String module, String object) {
        String modulesIdField = modulesDeclared.getFieldsDoQuery(module).get(0);
        String condition = modulesIdField + "='" + object + "'";
        String query = "Select * from " + module + " where " + condition;
        Object response = wsClient.doQuery(query);
        return response;
    }

}