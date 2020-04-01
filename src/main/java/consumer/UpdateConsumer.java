package consumer;

import helper.Util;
import model.KeyData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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


    private void readRecord(ConsumerRecord record) throws ParseException {
        System.out.println(String.format("Topic - %s, Key - %s, Partition - %d, Value: %s", record.topic(), record.key(), record.partition(), record.value()));
        KeyData keyData = Util.getObjectFromJson((String) record.key(), KeyData.class);
        Object value = Util.getObjectFromJson((String) record.value(), Object.class);
        if (Objects.requireNonNull(keyData).operation.equals(Util.methodUPDATE)) {
            System.out.println("Upserting the Record");
            upsertRecord(keyData.module, (Map) value);
        } else if (keyData.operation.equals(Util.methodDELETE)) {
            System.out.println("Deleting the Record");
            deleteRecord(keyData.module, (String) value);
        }
    }

    private void upsertRecord(String module, Map element) throws ParseException {
        Map<String, Object> mapToSend = new HashMap<>();
        Map<String, Object> fieldUpdate = new HashMap<>();

        if (Objects.equals(useFieldMapping, "yes")) {

            // Get All uitype 10 Module fields
            JSONObject module_info = wsClient.doDescribe(module);
            JSONArray reference_fields = (JSONArray) module_info.get("fields");
            Map<String, String> moduleFieldInfo = new HashMap<>();
            for (Object o : reference_fields) {
                JSONObject fieldInfo = (JSONObject) o;
                if (fieldInfo.containsKey("uitype") && Objects.equals(fieldInfo.get("uitype").toString(), "10")) {
                    JSONObject typeObject = (JSONObject) fieldInfo.get("type");
                    JSONArray referenceTo = (JSONArray) typeObject.get("refersTo");
                    if (referenceTo.size() == 1) {
                        moduleFieldInfo.put(fieldInfo.get("name").toString(), referenceTo.get(0).toString());
                    }
                }
            }

            String mapName = "REST2" + module;
            String mapModule = "cbMap";
            String condition = "mapname" + "='" + mapName + "'";
            String queryMap = "select * from " + mapModule + " where " + condition;

            JSONArray mapdata = wsClient.doQuery(queryMap);
            JSONParser parser = new JSONParser();
            JSONObject result = (JSONObject)parser.parse(mapdata.get(0).toString());
            JSONObject contentjson = (JSONObject)parser.parse(result.get("contentjson").toString());
            JSONObject fields = (JSONObject)parser.parse(contentjson.get("fields").toString());
            JSONArray fields_array = (JSONArray) fields.get("field");
            for (Object field: fields_array) {
                JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                Object value = getFieldValue(originalFiled.get("OrgfieldName").toString(), element, moduleFieldInfo,
                        module, ((JSONObject)field).get("fieldname").toString());
                if (((JSONObject) value).get("status").toString().equals("found")) {
                    fieldUpdate.put(((JSONObject)field).get("fieldname").toString(), ((JSONObject)value).get("value"));
                }
            }

            fieldUpdate.put("assigned_user_id", wsClient.getUserID());
            mapToSend.put("elementType", module);
            mapToSend.put("element", Util.getJson(fieldUpdate));
            mapToSend.put("searchOn", fieldsDoQuery);
        } else {
            String modulesIdField = Objects.requireNonNull(modulesDeclared).getFieldsDoQuery(module).get(0);
            for (String filedl : modulesDeclared.getFieldsConsiderate(module))
                fieldUpdate.put(filedl, element.get(filedl));
            fieldUpdate.put("assigned_user_id", wsClient.getUserID());
            fieldUpdate.put(modulesIdField, element.get("id"));

            mapToSend.put("elementType", module);
            mapToSend.put("element", Util.getJson(fieldUpdate));
            mapToSend.put("searchOn", modulesIdField);
        }

        Object d = wsClient.doInvoke(Util.methodUPSERT, mapToSend, "POST");
        System.out.println("Util.getJson(d) = " + Util.getJson(d));

    }

    @SuppressWarnings("unchecked")
    private Object getFieldValue(String orgfieldName, Map element, Map<String, String> moduleFieldInfo,
                                 String parentModule, String fieldname) throws ParseException {
        JSONObject rs = new JSONObject();
        JSONObject record = new JSONObject();
        record.putAll(element);
        if(record.containsKey(orgfieldName)) {
            // Check if the field value can be converted to JSONArray or JSONObject
            String jsonValue = Util.getJson(record.get(orgfieldName));
            JSONParser parser = new JSONParser();
            if (parser.parse(jsonValue) instanceof JSONObject) {
                 Map<String, Object> fieldToSearch = getSearchField(parentModule);
                 Map<String, Object> searchResult = searchRecord(moduleFieldInfo.get(fieldname),
                        ((JSONObject) parser.parse(jsonValue)).get("ID").toString(),
                        fieldToSearch.get(orgfieldName).toString());

                 if (((boolean) searchResult.get("status"))) {
                     rs.put("status", "found");
                     rs.put("value",  searchResult.get("crmid"));
                 } else {
                     // We Have to Create the New Record and get Its CRMID
                     Map<String, Object> recordMap = new HashMap<>();
                     Map<String, Object> recordField = new HashMap<>();
                     //1. Get Map for Adding that Module from Rest API
                     String mapName = "REST2" + moduleFieldInfo.get(fieldname);
                     String mapModule = "cbMap";
                     String condition = "mapname" + "='" + mapName + "'";
                     String queryMap = "select * from " + mapModule + " where " + condition;

                     JSONArray mapdata = wsClient.doQuery(queryMap);
                     JSONObject result = (JSONObject)parser.parse(mapdata.get(0).toString());
                     JSONObject contentjson = (JSONObject)parser.parse(result.get("contentjson").toString());
                     JSONObject fields = (JSONObject)parser.parse(contentjson.get("fields").toString());
                     JSONArray fields_array = (JSONArray) fields.get("field");
                     for (Object field: fields_array) {
                         JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                         JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                         Object value = getFieldValue(originalFiled.get("OrgfieldName").toString(), element, moduleFieldInfo, parentModule, fieldname);
                         if (((JSONObject) value).get("status").toString().equals("found")) {
                             recordField.put(((JSONObject)field).get("fieldname").toString(), ((JSONObject)value).get("value"));
                         }
                     }

                     recordField.put("assigned_user_id", wsClient.getUserID());
                     recordMap.put("elementType", moduleFieldInfo.get(fieldname));
                     recordMap.put("element", Util.getJson(recordField));
                     recordMap.put("searchOn", fieldToSearch.get(orgfieldName));
                     Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMap, "POST");
                     JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                     if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                         rs.put("status", "found");
                         rs.put("value",  obj.get("id").toString());
                     }
                 }
            } else if (parser.parse(jsonValue) instanceof JSONArray) {
                JSONArray recordsArray = (JSONArray) parser.parse(jsonValue);
                for (Object objrecord : recordsArray) {
                    Map<String, Object> fieldToSearch = getSearchField(parentModule);
                    Map<String, Object> searchResult = searchRecord(moduleFieldInfo.get(fieldname),
                            ((JSONObject) parser.parse(String.valueOf(objrecord))).get("ID").toString(),
                            fieldToSearch.get(orgfieldName).toString());

                    if (((boolean) searchResult.get("status"))) {
                        rs.put("status", "found");
                        rs.put("value",  searchResult.get("crmid"));
                    } else {
                        // We Have to Create the New Record and get Its CRMID
                        Map<String, Object> recordMap = new HashMap<>();
                        Map<String, Object> recordField = new HashMap<>();
                        //1. Get Map for Adding that Module from Rest API
                        String mapName = "REST2" + moduleFieldInfo.get(fieldname);
                        String mapModule = "cbMap";
                        String condition = "mapname" + "='" + mapName + "'";
                        String queryMap = "select * from " + mapModule + " where " + condition;

                        JSONArray mapdata = wsClient.doQuery(queryMap);
                        JSONObject result = (JSONObject)parser.parse(mapdata.get(0).toString());
                        JSONObject contentjson = (JSONObject)parser.parse(result.get("contentjson").toString());
                        JSONObject fields = (JSONObject)parser.parse(contentjson.get("fields").toString());
                        JSONArray fields_array = (JSONArray) fields.get("field");
                        for (Object field: fields_array) {
                            JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                            JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                            Object value = getFieldValue(originalFiled.get("OrgfieldName").toString(), element, moduleFieldInfo, parentModule, fieldname);
                            if (((JSONObject) value).get("status").toString().equals("found")) {
                                recordField.put(((JSONObject)field).get("fieldname").toString(), ((JSONObject)value).get("value"));
                            }
                        }

                        recordField.put("assigned_user_id", wsClient.getUserID());
                        recordMap.put("elementType", moduleFieldInfo.get(fieldname));
                        recordMap.put("element", Util.getJson(recordField));
                        recordMap.put("searchOn", fieldToSearch.get(orgfieldName));
                        Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMap, "POST");
                        JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                        if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                            rs.put("status", "found");
                            rs.put("value",  obj.get("id").toString());
                        }
                    }
                }
            } else {
                rs.put("status", "found");
                rs.put("value",  record.get(orgfieldName));
            }
            return rs;
        }else {
            for (Object o : record.entrySet()) {
                rs.clear();
                Map.Entry<String, Object> entry = (Map.Entry<String, Object>) o;
                if (entry.getKey().equals(orgfieldName)) {
                    rs.put("status", "found");
                    rs.put("value",  entry.getValue());
                    return rs;
                } else {
                    String jsonValue= Util.getJson( entry.getValue());
                    JSONParser parser = new JSONParser();
                    if (parser.parse(jsonValue) instanceof JSONObject) {
                        System.out.println("Instance of JSONObject");
                        rs = handleJSONObject((JSONObject) parser.parse(jsonValue), orgfieldName);
                        if (rs.get("status").toString().equals("found")) {
                            return rs;
                        }
                    } else if (parser.parse(jsonValue) instanceof JSONArray) {
                        System.out.println("Instance of JSONArray");
                        rs = handleJSONArray((JSONArray) parser.parse(jsonValue), orgfieldName);
                        if (rs.get("status").toString().equals("found")) {
                            return rs;
                        }
                    }
                }
            }
        }

        rs.put("status", "notfound");
        rs.put("value",  "");
        return rs;
    }

    @SuppressWarnings("unchecked")
    private JSONObject handleJSONObject(JSONObject jsonObject, String orgfieldName) {
        JSONObject rs = new JSONObject();
        for (Object o : jsonObject.entrySet()) {
            Map.Entry<String, Object> entry = (Map.Entry<String, Object>) o;
            if (entry.getKey().equals(orgfieldName)) {
                rs.put("status", "found");
                rs.put("value",  entry.getValue());
                return rs;
            }
        }
        rs.put("status", "notfound");
        rs.put("value",  "");
        return rs;
    }

    @SuppressWarnings("unchecked")
    private JSONObject handleJSONArray(JSONArray jsonArray, String orgfieldName) {
        JSONObject rs = new JSONObject();
        for (Object o : jsonArray) {
            JSONObject jsonLineItem = (JSONObject) o;
           return handleJSONObject(jsonLineItem, orgfieldName);
        }
        rs.put("status", "notfound");
        rs.put("value",  "");
        return rs;
    }

    private Map<String, Object> getSearchField(String parentModule) throws ParseException {
        JSONParser parser = new JSONParser();
        Map<String, Object> fieldmap= new HashMap<>();
        String mapName = "RESTSEARCH2" + parentModule;
        String objectModule = "cbMap";
        String condition = "mapname" + "='" + mapName + "'";
        String queryMap = "select *from " + objectModule + " where " + condition;
        JSONArray mapdata = wsClient.doQuery(queryMap);
        //System.out.println(queryMap);
        JSONObject result = (JSONObject)parser.parse(mapdata.get(0).toString());
        JSONObject contentjson = (JSONObject)parser.parse(result.get("contentjson").toString());
        JSONObject fields = (JSONObject)parser.parse(contentjson.get("fields").toString());
        JSONArray fields_array = new JSONArray();
        if (!(fields.get("field") instanceof JSONArray)) {
            fields_array.add(fields.get("field"));
        } else {
            fields_array = (JSONArray) fields.get("field");
        }

        for (Object field: fields_array) {
            JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
            JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                fieldmap.put(((JSONObject)field).get("fieldname").toString(), originalFiled.get("OrgfieldName").toString());
        }
        return fieldmap;
    }

    private Map<String, Object> searchRecord(String module, String value, String fieldname) throws ParseException {
        Map<String, Object> result = new HashMap<>();
        String condition = fieldname + "='" + value + "'";
        String queryMap = "select * from " + module + " where " + condition;

        JSONArray mapdata = wsClient.doQuery(queryMap);
        if (mapdata.size() == 0) {
            result.put("status", false);
            result.put("crmid", "");
        } else {
            JSONParser parser = new JSONParser();
            JSONObject queryResult = (JSONObject)parser.parse(mapdata.get(0).toString());
            String crmid = String.valueOf(parser.parse(queryResult.get("id").toString()));
            if (!crmid.isEmpty()) {
                result.put("status", true);
            } else {
                result.put("status", false);
            }
            result.put("crmid", crmid);
        }

        return result;
    }

    private Object createRecord() {

       // Object record = wsClient.doInvoke(Util.methodUPSERT, mapToSend, "POST");
        //return record;
        return null;
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