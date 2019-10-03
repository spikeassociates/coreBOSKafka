package consumer;

import helper.Util;
import model.KeyData;
import model.Modules;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import producer.SyncProducer;

import java.util.*;

public class UpdateConsumer extends Consumer {

    Object[] modules;
    final Modules modulesDeclarate = Util.getObjectFromJson(Util.getProperty("corebos.test.modules"), Modules.class);

    public UpdateConsumer() throws Exception {
        modules = wsClient.doDescribe(MODULES).values().toArray();
        List topics = new ArrayList();
        topics.add("first_topic");
        kafkaConsumer.subscribe(topics);
    }

    public void init() {

        try {
            while (true) {
                try {
                    //todo comment in demo Config.getInstance().save();
                    //todo put in demo mapToSend.put("modifiedTime", "1570105020");
                    new SyncProducer().init();
                } catch (Exception e) {
                    System.out.println(e);
                    e.printStackTrace();
                }
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

    private void updateRecord(ConsumerRecord record) {
        System.out.println(String.format("Topic - %s, Key - %s, Partition - %d, Value: %s", record.topic(), record.key(), record.partition(), record.value()));
        KeyData keyData = Util.getObjectFromJson((String) record.key(), KeyData.class);
        if (!keyData.operation.equals(Util.methodUPDATE))
            return;
        Object value = Util.getObjectFromJson((String) record.value(), Object.class);
        Object response = getRecord(keyData.module, value);
        if (response == null || ((List) response).size() == 0) {
            createRecord(keyData.module, (Map) value);
        } else {
            doUpdate(keyData.module, (Map) value, (Map) ((List) response).get(0));
        }
        System.out.println(record.toString());
    }

    private Object getRecord(String module, Object object) {
        ArrayList<String> fields = modulesDeclarate.getFieldsDoQuery(module);
        String con = "";
        boolean firstCondition = true;
        for (String field : fields) {
            String val = (String) ((Map) object).get(field);
            if (val == null || val.equals(""))
                continue;
            if (firstCondition) {
                con += " " + field + "=\'" + ((Map) object).get(field) + "\' ";
            } else {
                con += " or " + field + "=\'" + ((Map) object).get(field) + "\' ";
            }
            firstCondition = false;
        }
//        no condition found record does not exist by requirements
        if (firstCondition)
            return null;
        String condition = "lastname='" + ((Map) object).get("lastname") + "'";
        String query = "Select * from " + module + " where " + condition;
        String qur = "Select * from " + module + " where " + con;
        Object response = wsClient.doQuery(qur);
        return response;
    }

    private void doUpdate(String module, Map originObject, Map destinationObject) {

        Map<String, Object> mapToSend = new HashMap<>();
        if (!modulesDeclarate.exist(module))
            return;

        for (String field : modulesDeclarate.getFieldsConsiderate(module)) {
            destinationObject.put(field, originObject.get(field));
        }
        mapToSend.put("elementType", module);
        mapToSend.put("element", Util.getJson(destinationObject));

        Object d = wsClient.doInvoke(Util.methodUPDATE, mapToSend, "POST");
        System.out.println("Util.getJson(d) = " + Util.getJson(d));

    }

    private void createRecord(String module, Map originObject) {
        Map<String, Object> mapToSend = new HashMap<>();

        originObject.remove("id");
        mapToSend.put("elementType", module);
        mapToSend.put("element", Util.getJson(originObject));


        Object d = wsClient.doInvoke(Util.methodCREATE, mapToSend, "POST");
        System.out.println("Util.getJson(d) = " + Util.getJson(d));

    }

}