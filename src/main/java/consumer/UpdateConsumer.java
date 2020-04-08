package consumer;

import helper.Util;
import model.KeyData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

public class UpdateConsumer extends Consumer {

    private final String topic = Util.getProperty("corebos.consumer.topic");
    private ArrayList<Map<String, Object>> lastRecordToCreate = new ArrayList<>();
    private Map<String, String> uitype10fields = new HashMap<>();
    private Map<String, String> moduleDateFields = new HashMap<>();

    public UpdateConsumer() throws Exception {
        List topics = new ArrayList();
        topics.add(topic);
        kafkaConsumer.subscribe(topics);
    }

    public void init() {

        try {
            while (true) {
                ConsumerRecords records = kafkaConsumer.poll(Duration.ofMillis(1000));
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
        //System.out.println(String.format("Topic - %s, Key - %s, Partition - %d, Value: %s", record.topic(), record.key(),record.partition(), record.value()));
        KeyData keyData = Util.getObjectFromJson((String) record.key(), KeyData.class);
        Object value = Util.getObjectFromJson((String) record.value(), Object.class);
        if (Objects.requireNonNull(keyData).operation.equals(Util.methodUPDATE)) {
            System.out.println("Upserting the Record");
            lastRecordToCreate.clear();
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
            // Module Special field e.g Date, Datetime
            getModuleDateFields(module);
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
        //System.out.println("Util.getJson(d) = " + Util.getJson(d));
        // We Nee to Create Other Module Record which depend on this Created Record
        Map<String, String> moduleCRMID = new HashMap<>();
        JSONParser parser = new JSONParser();
        JSONObject createdRecord = (JSONObject)parser.parse(Util.getJson(d));
        moduleCRMID.put(module, createdRecord.get("id").toString());
        createRecordsInMap(moduleCRMID);


    }

    @SuppressWarnings("unchecked")
    private Object getFieldValue(String orgfieldName, Map element, Map<String, String> moduleFieldInfo,
                                 String parentModule, String fieldname) throws ParseException {
        JSONObject rs = new JSONObject();
        JSONObject record = new JSONObject();
        record.putAll(element);
        if(record.containsKey(orgfieldName)) {
            /*
            1. Check if the field value can be converted to JSONArray or JSONObject
            2. If is Object or JSON Array check if it exit in Module field search and is one of the module field
            3. if the above statement if false means the JSONArray or JSONObject record depend on the Main Module Record whihc is about to be created
            */
            String jsonValue = Util.getJson(record.get(orgfieldName));
            JSONParser parser = new JSONParser();
            if (parser.parse(jsonValue) instanceof JSONObject) {
                //Get the Search fields
                 Map<String, Object> fieldToSearch = getSearchField(parentModule);
                 if (!fieldToSearch.isEmpty() && moduleFieldInfo.containsKey(fieldname)) {
                     Map<String, Object> searchResult = searchRecord(moduleFieldInfo.get(fieldname), ((JSONObject) parser.parse(jsonValue)).get("ID").toString(), fieldToSearch.get(orgfieldName).toString());
                     if (((boolean) searchResult.get("status"))) {
                         rs.put("status", "found");
                         rs.put("value",  searchResult.get("crmid"));
                     } else {
                         // We Have to Create the New Record and get Its CRMID
                         Map<String, Object> recordMap = new HashMap<>();
                         Map<String, Object> recordField = new HashMap<>();
                         //1. Get Map for Adding that Module from Rest API
                         String mapName = orgfieldName + "2" + moduleFieldInfo.get(fieldname);
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
                                 if (moduleDateFields.containsKey(fieldname) &&
                                         (moduleDateFields.get(fieldname).equals("5") ||
                                                 moduleDateFields.get(fieldname).equals("50"))) {
                                     String dateValue = ((JSONObject)value).get("value").toString().replace("T", " ");
                                     recordField.put(((JSONObject)field).get("fieldname").toString(), dateValue);
                                 } else {
                                     recordField.put(((JSONObject)field).get("fieldname").toString(), ((JSONObject)value).get("value"));
                                 }
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
                         } else {
                             rs.put("status", "notfound");
                             rs.put("value",  "");
                         }
                         return rs;
                     }
                 } else {
                     // This Module contain reference field which depend on Main Module
                     uitype10fields = getUIType10Field(fieldname);
                     if (!uitype10fields.isEmpty()) {
                         Map<String, Object> objValue = (Map<String, Object>) element.get(orgfieldName);
                         Map<String, Object> recordToCreate =  getMapOfRecordToBeCreated(uitype10fields, fieldname,
                                 parentModule, objValue, fieldToSearch.get(orgfieldName).toString(), orgfieldName);
                         lastRecordToCreate.add(recordToCreate);
                     } else {
                         // TODO: 4/8/20 Handle for module which do not contain any reference field
                         // Create a Module Record which do not contain reference fields
                     }

                     rs.put("status", "notfound");
                     rs.put("value",  "");
                     return rs;
                 }
            } else if (parser.parse(jsonValue) instanceof JSONArray) {
                // TODO: 4/8/20 Handle Object and Array if value contain those data
                // Note: Here we assumed that the object inside the array its key value pair and its value is never Object or an Array
                // If we Need to Handle for Object and Array we Need to just add very very simple code which is already implemented
                JSONArray recordsArray = (JSONArray) parser.parse(jsonValue);
                Map<String, Object> fieldToSearch = getSearchField(parentModule);
                for (Object objRecord : recordsArray) {
                    if (objRecord instanceof JSONObject) {
                        uitype10fields = getUIType10Field(fieldname);
                        if (!uitype10fields.isEmpty()) {
                            Map<String, Object> objValue = (Map<String, Object>) objRecord;
                            Map<String, Object> recordToCreate =  getMapOfRecordToBeCreated(uitype10fields, fieldname,
                                    parentModule, objValue, fieldToSearch.get(orgfieldName).toString(), orgfieldName);
                            lastRecordToCreate.add(recordToCreate);
                        } else {
                            // TODO: 4/8/20 Handle for Module which do not contain any reference field
                            // Create a Module Record which do not contain reference fields
                        }
                    }
                }
                rs.put("status", "notfound");
                rs.put("value",  "");
                return rs;
            }
            else {
                rs.put("status", "found");
                if (moduleDateFields.containsKey(fieldname) && (moduleDateFields.get(fieldname).equals("5") || moduleDateFields.get(fieldname).equals("50"))) {
                    String dateValue = record.get(orgfieldName).toString().replace("T", " ");
                    rs.put("value",  dateValue);
                } else {
                    rs.put("value",  record.get(orgfieldName));
                }
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
                        rs = handleJSONObject((JSONObject) parser.parse(jsonValue), orgfieldName);
                        if (rs.get("status").toString().equals("found")) {
                            return rs;
                        }
                    } else if (parser.parse(jsonValue) instanceof JSONArray) {
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
        if (mapdata.size() == 0) {
            return fieldmap;
        } else {
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
        }
        return fieldmap;
    }

    private Map<String, Object> searchRecord(String module, String value, String fieldname) throws ParseException {
        Map<String, Object> result = new HashMap<>();
        String condition = fieldname + "='" + value + "'";
        String queryMap = "select * from " + module + " where " + condition;
        //System.out.println(queryMap);
        JSONArray mapdata = wsClient.doQuery(queryMap);
        //System.out.println(mapdata);
        if (mapdata.size() == 0) {
            result.put("status", false);
            result.put("crmid", "");
        } else {
            JSONParser parser = new JSONParser();
            JSONObject queryResult = (JSONObject)parser.parse(mapdata.get(0).toString());
            //System.out.println(queryResult);
            //System.out.println(queryResult.get("id").toString());
            String crmid = queryResult.get("id").toString();
            //System.out.println(crmid);
            if (!crmid.isEmpty()) {
                result.put("status", true);
            } else {
                result.put("status", false);
            }
            result.put("crmid", crmid);
        }
        return result;
    }

    private Map<String, String> getUIType10Field(String module) {
        // Get All uitype 10 Module fields
        JSONObject module_info = wsClient.doDescribe(module);
        JSONArray reference_fields = (JSONArray) module_info.get("fields");
        Map<String, String> uitype10fields = new HashMap<>();
        for (Object o : reference_fields) {
            JSONObject fieldInfo = (JSONObject) o;
            if (fieldInfo.containsKey("uitype") && Objects.equals(fieldInfo.get("uitype").toString(), "10")) {
                JSONObject typeObject = (JSONObject) fieldInfo.get("type");
                JSONArray referenceTo = (JSONArray) typeObject.get("refersTo");
                if (referenceTo.size() == 1) {
                    uitype10fields.put(fieldInfo.get("name").toString(), referenceTo.get(0).toString());
                }
            }
        }

        return uitype10fields;
    }

    private void getModuleDateFields(String module) {
        JSONObject module_info = wsClient.doDescribe(module);
        JSONArray reference_fields = (JSONArray) module_info.get("fields");
        for (Object o : reference_fields) {
            JSONObject fieldInfo = (JSONObject) o;
            if (fieldInfo.containsKey("uitype") && (Objects.equals(
                    fieldInfo.get("uitype").toString(), "50") || Objects.equals(
                            fieldInfo.get("uitype").toString(), "5"))) {
                this.moduleDateFields.put(fieldInfo.get("name").toString(), fieldInfo.get("uitype").toString());
            }
        }
        //System.out.println("Get Module Date Fields");
        //System.out.println(this.moduleDateFields);
    }

    private Map<String, Object> getMapOfRecordToBeCreated(Map<String, String> moduleFieldInfo, String fieldname,
                                                          String parentModule, Map element, String fieldToSearch,
                                                          String orgfieldName) throws ParseException {
        // We Have to Create the New Record and get Its CRMID
        // String modulesIdField = Objects.requireNonNull(modulesDeclared).getFieldsDoQuery(moduleFieldInfo.get(fieldname)).get(0);
        Map<String, Object> recordMap = new HashMap<>();
        Map<String, Object> recordField = new HashMap<>();
        JSONParser parser = new JSONParser();
        // Get Map for Adding that Module from Rest API
        String mapName = orgfieldName + "2" + fieldname;
        System.out.println(mapName);
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
                if (moduleDateFields.containsKey(fieldname) && (moduleDateFields.get(fieldname).equals("5") || moduleDateFields.get(fieldname).equals("50"))) {
                    String dateValue = element.get(originalFiled.get("OrgfieldName")).toString().replace("T", " ");
                    recordField.put(((JSONObject)field).get("fieldname").toString(), dateValue);
                } else {
                    recordField.put(((JSONObject)field).get("fieldname").toString(), element.get(originalFiled.get("OrgfieldName").toString()));
                }
            }
        }

        recordField.put("assigned_user_id", wsClient.getUserID());
        recordMap.put("elementType", fieldname);
        recordMap.put("element", Util.getJson(recordField));
        recordMap.put("searchOn", fieldToSearch);

        return recordMap;
    }

    private void createRecordsInMap(Map<String, String> moduleCRMID) throws ParseException {
        for (Map<String, Object> record: lastRecordToCreate
             ) {
            String module = record.get("elementType").toString();
            Map<String, String> uitype10fields = getUIType10Field(module);
            JSONParser parser = new JSONParser();
            JSONObject recordFields = (JSONObject) parser.parse(record.get("element").toString());
            System.out.println("Element:: " + recordFields);
            System.out.println("Uitype10fields:: " + uitype10fields);
            System.out.println("Module CRMID:: " + moduleCRMID);
            for (Object key : uitype10fields.keySet()) {
                String keyStr = (String)key;
                //if (recordFields.containsKey(keyStr)) {
                    if (moduleCRMID.containsKey(uitype10fields.get(keyStr))) {
                        // set the field value
                        recordFields.put(keyStr, moduleCRMID.get(uitype10fields.get(keyStr)));
                    } else {
                        // remove the field on the Map to Send
                        //recordFields.remove(keyStr);
                    }
                //}

            }
            record.put("element", Util.getJson(recordFields));
            System.out.println("Util.getJson(d) for Child Record = " + record);
            Object d = wsClient.doInvoke(Util.methodUPSERT, record, "POST");
            System.out.println("Util.getJson(d) for Child Record = " + Util.getJson(d));
        }

    }

    private static String encodeValue(Object value) {
        try {
            return URLEncoder.encode(String.valueOf(value), StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex.getCause());
        }
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