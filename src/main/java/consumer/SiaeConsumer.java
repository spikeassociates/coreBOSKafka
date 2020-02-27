package consumer;

import helper.Util;
import model.SiaeKeyData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import producer.SiaeProducer;

import java.util.*;

public class SiaeConsumer extends Consumer {

    final String fromApi = Util.getProperty("corebos.siae.from_API");
    final String signed_topic = Util.getProperty("corebos.siae.signed_topic");
    final String get_topic = Util.getProperty("corebos.siae.get_topic");
    final String notify_topic = Util.getProperty("corebos.siae.notify_topic");
    SiaeProducer producer;

    public SiaeConsumer() throws Exception {
        producer = new SiaeProducer();
        List topics = new ArrayList();
        topics.add(fromApi);
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
                    updateRecord(record);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }

    private void updateRecord(ConsumerRecord record) throws Exception {
        System.out.println(String.format("Topic - %s, Key - %s, Partition - %d, Value: %s", record.topic(), record.key(), record.partition(), record.value()));
        SiaeKeyData keyData = Util.getObjectFromJson((String) record.key(), SiaeKeyData.class);
        if (record.topic().equals(get_topic)) {
            getCBModule(keyData);
            return;
        }
        Object value = Util.getObjectFromJson((String) record.value(), Object.class);
        upsertRecord(keyData.module, (Map) value);

    }

    private void getCBModule(SiaeKeyData keyData) {
//        Object response = wsClient.doRetrieve();

        producer.publishMessage(notify_topic, Util.getJson(keyData), "the message GEt");
    }


    private void upsertRecord(String module, Map element) {
        String modulesIdField = "id";
        Map<String, Object> mapToSend = new HashMap<>();

        element.put("assigned_user_id", wsClient.getUserID());

        mapToSend.put("elementType", module);
        mapToSend.put("element", Util.getJson(element));
        mapToSend.put("searchOn", modulesIdField);

        Object d = wsClient.doInvoke(Util.methodUPSERT, mapToSend, "POST");
        System.out.println("Util.getJson(d) = " + Util.getJson(d));

    }


}