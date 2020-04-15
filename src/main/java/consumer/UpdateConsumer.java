package consumer;

import helper.Util;
import model.KeyData;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import service.RESTClient;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

public class UpdateConsumer extends Consumer {

    private final String topic = Util.getProperty("corebos.consumer.topic");
    private final String rest_api_url = Util.getProperty("corebos.restproducer.url");
    protected static final String username = Util.getProperty("corebos.restproducer.username");
    protected static final String password = Util.getProperty("corebos.restproducer.password");
    private final String auth_endpoint = Util.getProperty("corebos.restproducer.authendpoint");
    private ArrayList<Map<String, Object>> lastRecordToCreate = new ArrayList<>();
    private Map<String, String> uitype10fields = new HashMap<>();
    private Map<String, String> moduleDateFields = new HashMap<>();
    protected RESTClient restClient;

    public UpdateConsumer() throws Exception {
        List topics = new ArrayList();
        topics.add(topic);
        kafkaConsumer.subscribe(topics);
    }

    public void init() {

        try {
            while (true) {
                ConsumerRecords records = kafkaConsumer.poll(Duration.ofMillis(2000));
                Iterator it = records.iterator();
                while (it.hasNext()) {
                    ConsumerRecord record = (ConsumerRecord) it.next();
                    readRecord(record);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            // kafkaConsumer.close();
        }
    }


    private void readRecord(ConsumerRecord record) throws Exception {
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

    private void upsertRecord(String module, Map element) throws Exception {
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

        //System.out.println("Map to Send" +  mapToSend);

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
                                 String parentModule, String fieldname) throws Exception {
        JSONObject rs = new JSONObject();
        JSONObject record = new JSONObject();
        record.putAll(element);
        //System.out.println(orgfieldName);
        System.out.println(fieldname);
        if(record.containsKey(orgfieldName) || orgfieldName.equals("distribuzioneFornitoreId") ||
                orgfieldName.equals("raeeFornitoreId")) {
            //System.out.println("MAMA JIKOLA");
            /*
            1. Check if the field value can be converted to JSONArray or JSONObject
            2. If is Object or JSON Array check if it exit in Module field search and is one of the module field
            3. if the above statement if false means the JSONArray or JSONObject record depend on the Main Module Record which is about to be created
            */
            String jsonValue = Util.getJson(record.get(orgfieldName));
            //System.out.println("MAMA ubwabwa");
            JSONParser parser = new JSONParser();
            //System.out.println((parser.parse(jsonValue) instanceof JSONObject));
            if ((parser.parse(jsonValue) instanceof JSONObject) ||  orgfieldName.equals("distribuzioneFornitoreId") ||
                    orgfieldName.equals("raeeFornitoreId")) {
                //System.out.println("IMEPENYA");
                //Get the Search fields
                 Map<String, Object> fieldToSearch = getSearchField(parentModule);
                //System.out.println(fieldToSearch);
                //System.out.println(moduleFieldInfo);
                 if (!fieldToSearch.isEmpty() && moduleFieldInfo.containsKey(fieldname)) {
                     //System.out.println("UNYANWEZUUUU");
                     String searchID;
                     if (orgfieldName.equals("distribuzioneFornitoreId") || orgfieldName.equals("raeeFornitoreId")) {
                         //System.out.println("UKUKU DANGER");
                         JSONObject importoSpedizione = (JSONObject) parser.parse(Util.getJson(record.get("importoSpedizione")));
                         //System.out.println(importoSpedizione.isEmpty());
                         //System.out.println(importoSpedizione);
                         if (importoSpedizione == null) {
                             rs.put("status", "notfound");
                             rs.put("value",  "");
                             return rs;
                         }
                         if (orgfieldName.equals("distribuzioneFornitoreId")) {
                            searchID = importoSpedizione.get("distribuzioneFornitoreId").toString();
                         } else {
                             searchID = importoSpedizione.get("raeeFornitoreId").toString();
                         }
                     } else {
                         searchID = ((JSONObject) parser.parse(jsonValue)).get("ID").toString();
                     }
                     System.out.println(searchID);
                     Map<String, Object> searchResult = searchRecord(moduleFieldInfo.get(fieldname), searchID,
                             fieldToSearch.get(orgfieldName).toString());
                     if (((boolean) searchResult.get("status"))) {
                         rs.put("status", "found");
                         rs.put("value",  searchResult.get("crmid"));
                     } else {
                         // for Special field key distribuzioneFornitore Id, raeeFornitoreId
                         if (orgfieldName.equals("distribuzioneFornitoreId") || orgfieldName.equals("raeeFornitoreId")) {
                             if (startRestService()) {
                                 if (((JSONObject) parser.parse(jsonValue)).get("importoSpedizione") != null) {
                                     String endpoint = "fornitori";
                                     String objectKey = "fornitori";
                                     String id = "";
                                     JSONObject importoSpedizione = (JSONObject) parser.parse(Util.getJson(record.get("importoSpedizione")));
                                     if (orgfieldName.equals("distribuzioneFornitoreId")) {
                                         // Search in Rest Api
                                         id = importoSpedizione.get("distribuzioneFornitoreId").toString();
                                         //endpoint = endpoint + "/" + id;
                                     } else if(fieldname.equals("raeeFornitoreId")) {
                                         id = importoSpedizione.get("raeeFornitoreId").toString();
                                         //endpoint = endpoint + "/" + id;
                                     }

                                     Object fonitoriResponse = doGet(restClient.get_servicetoken(), endpoint, objectKey);
                                     if (fonitoriResponse == null) {
                                         rs.put("status", "notfound");
                                         rs.put("value",  "");
                                         return rs;
                                     }
                                     Map<String, Object> fonitoriObject = searchByID(fonitoriResponse, id);

                                     // Map to Create the Record
                                     Map<String, Object> recordMap = new HashMap<>();
                                     Map<String, Object> recordField = new HashMap<>();
                                     String mapName = orgfieldName + "2" + fieldname;
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
//                                         if (moduleDateFields.containsKey(fieldname) && (moduleDateFields.get(fieldname).equals("5") || moduleDateFields.get(fieldname).equals("50"))) {
//                                             String dateValue = fonitoriObject.get(originalFiled.get("OrgfieldName")).toString().replace("T", " ");
//                                             recordField.put(((JSONObject)field).get("fieldname").toString(), dateValue);
//                                         } else {
                                             //assert fonitoriObject != null;
                                             recordField.put(((JSONObject)field).get("fieldname").toString(), fonitoriObject.get(originalFiled.get("OrgfieldName").toString()));
//                                         }
                                     }

                                     recordField.put("assigned_user_id", wsClient.getUserID());
                                     recordField.put("type", "Fornitore");
                                     recordMap.put("elementType", fieldname);
                                     recordMap.put("element", Util.getJson(recordField));
                                     recordMap.put("searchOn", fieldToSearch);
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
                                 } else {
                                     rs.put("status", "notfound");
                                     rs.put("value",  "");
                                 }
                             }
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
                                 Object value = getFieldValue(originalFiled.get("OrgfieldName").toString(), (Map) record.get(orgfieldName), moduleFieldInfo, parentModule, fieldname);
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


                             // Handle Special Case for indirizzoMittente, indirizzoRitiro, indirizzoConsegna, indirizzoDestinatario
                             if (orgfieldName.equals("indirizzoMittente") || orgfieldName.equals("indirizzoRitiro") ||
                                     orgfieldName.equals("indirizzoConsegna") || orgfieldName.equals("indirizzoDestinatario")) {
                                 /*
                                  * Query Geoboundary Module  where geoname == comune
                                  */
                                 System.out.println("Current Value::" + ((JSONObject) parser.parse(jsonValue)).get("comune").toString());
                                 Map<String, Object> searchResultGeoboundary = searchRecord("Geoboundary",
                                         ((JSONObject) parser.parse(jsonValue)).get("comune").toString(),
                                         "geoname");
                                 System.out.println("searchResultGeoboundary:::" + searchResultGeoboundary);
                                 if (((boolean) searchResultGeoboundary.get("status"))) {
                                     Map<String, String> referenceFields = getUIType10Field(moduleFieldInfo.get(fieldname));
                                     System.out.println("Reference Field::" + referenceFields);
                                     for (Object key : referenceFields.keySet()) {
                                         String keyStr = (String)key;
                                         System.out.println("Key String::" + keyStr);
                                         if (referenceFields.get(keyStr).equals("GeoBoundary")) {
                                             System.out.println("value::" + searchResultGeoboundary.get("crmid"));
                                             recordField.put(keyStr, searchResultGeoboundary.get("crmid"));
                                             System.out.println("Record Field::" + recordField);
                                         }
                                     }

                                 }
                             }

                             recordField.put("assigned_user_id", wsClient.getUserID());
                             recordMap.put("elementType", moduleFieldInfo.get(fieldname));
                             recordMap.put("element", Util.getJson(recordField));
                             recordMap.put("searchOn", fieldToSearch.get(orgfieldName));
                             Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMap, "POST");
                             JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                             //System.out.println(obj.get("id").toString());
                             if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                                 rs.put("status", "found");
                                 rs.put("value",  obj.get("id").toString());
                             } else {
                                 rs.put("status", "notfound");
                                 rs.put("value",  "");
                             }
                             return rs;
                         }
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
                //System.out.println(parser.parse(jsonValue));
                JSONArray recordsArray = (JSONArray) parser.parse(jsonValue);
                Map<String, Object> fieldToSearch = getSearchField(parentModule);
                //System.out.println(fieldToSearch);
                for (Object objRecord : recordsArray) {
                    if (objRecord instanceof JSONObject) {
                        uitype10fields = getUIType10Field(fieldname);
                        //System.out.println(uitype10fields);
                        //System.out.println(objRecord);
                        if (!uitype10fields.isEmpty()) {
                            Map<String, Object> objValue = (Map<String, Object>) objRecord;
                            //System.out.println(objValue);
                            //System.out.println("TESSSSS");
                            //System.out.println(uitype10fields);
                            //System.out.println(fieldname);
                            //System.out.println(parentModule);
                            //System.out.println(objValue);
                            //System.out.println(fieldToSearch.get(orgfieldName).toString());
                            //System.out.println(orgfieldName);
                            String fldsearch = "";
                            if (fieldToSearch.containsKey(orgfieldName)) {
                                fldsearch = fieldToSearch.get(orgfieldName).toString();
                            }
                            Map<String, Object> recordToCreate =  getMapOfRecordToBeCreated(uitype10fields, fieldname,
                                    parentModule, objValue, fldsearch, orgfieldName);
                            //System.out.println("GIS::" + recordToCreate);
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
        // Check if value contain any Special character especially '

        if (value.contains("'")) {
            int specialCharPosition = value.indexOf("'") + 1;
            StringBuffer stringBuffer= new StringBuffer(value);
            value = stringBuffer.insert(specialCharPosition, "'").toString();
        }
        String condition;
        if (module.equals("Vendors")) {
            condition = fieldname + "='" + value + "'"  + "AND type ='Fornitore'";
        } else {
            condition = fieldname + "='" + value + "'";
        }
        String queryMap = "select * from " + module + " where " + condition;
        System.out.println(queryMap);
        JSONArray mapdata = wsClient.doQuery(queryMap);
        System.out.println(mapdata);
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
                                                          String orgfieldName) throws Exception {
        // We Have to Create the New Record and get Its CRMID
        // String modulesIdField = Objects.requireNonNull(modulesDeclared).getFieldsDoQuery(moduleFieldInfo.get(fieldname)).get(0);
        //System.out.println(moduleFieldInfo);
        //System.out.println(fieldname);
        //System.out.println(parentModule);
        //System.out.println(element);
        //System.out.println(fieldToSearch);
        //System.out.println(orgfieldName);

        Map<String, Object> recordMap = new HashMap<>();
        Map<String, Object> recordField = new HashMap<>();
        JSONParser parser = new JSONParser();
        // Get Map for Adding that Module from Rest API
        String mapName = orgfieldName + "2" + fieldname;
        //System.out.println(mapName);
        String mapModule = "cbMap";
        String condition = "mapname" + "='" + mapName + "'";
        String queryMap = "select * from " + mapModule + " where " + condition;
        //System.out.println(queryMap);
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

        //System.out.println(queryMap);
        recordField.put("assigned_user_id", wsClient.getUserID());
        recordMap.put("elementType", fieldname);
        recordMap.put("element", Util.getJson(recordField));
        recordMap.put("searchOn", fieldToSearch);
        //System.out.println(recordMap);
        return recordMap;
    }

    private void createRecordsInMap(Map<String, String> moduleCRMID) throws ParseException {
        for (Map<String, Object> record: lastRecordToCreate
             ) {
            String module = record.get("elementType").toString();
            Map<String, String> uitype10fields = getUIType10Field(module);
            JSONParser parser = new JSONParser();
            JSONObject recordFields = (JSONObject) parser.parse(record.get("element").toString());
            //System.out.println("Element:: " + recordFields);
            //System.out.println("Uitype10fields:: " + uitype10fields);
            //System.out.println("Module CRMID:: " + moduleCRMID);
            for (Object key : uitype10fields.keySet()) {
                String keyStr = (String)key;
                if (moduleCRMID.containsKey(uitype10fields.get(keyStr))) {
                    // set the field value
                    recordFields.put(keyStr, moduleCRMID.get(uitype10fields.get(keyStr)));
                } else {
                    //System.out.println("SEMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
                    //System.out.println("Module:: " + module);
                    //System.out.println("record:: " + record);
                    //System.out.println("key:: " + keyStr);
//                    System.out.println(recordFields.containsKey(keyStr));
//                    System.out.println(recordFields.get(keyStr).toString().equals(""));
//                    System.out.println( recordFields.get(keyStr).toString());
                    if (recordFields.containsKey(keyStr) && recordFields.get(keyStr) != null &&
                            recordFields.get(keyStr) != "") {

                        //System.out.println(recordFields.containsKey(keyStr));
                        //System.out.println(recordFields.get(keyStr));
                        //System.out.println( recordFields.get(keyStr).toString());
                        // TODO: 4/10/20 Scenario for Prodotti
                        //System.out.println("SUKARIIIIIIIIIIII YA WALEBOOOOOOOOOOOOOOOOOOOOO");
                        //Get field to search when we want to create module record
                        Map<String, Object> fieldToSearch = getSearchField(module);
                        //System.out.println("search:: " + uitype10fields.get(keyStr));
                        //System.out.println("search:: " + recordFields.get(keyStr));
                        //System.out.println("search:: " + fieldToSearch.get(keyStr));

                        Map<String, Object> searchResult = searchRecord(uitype10fields.get(keyStr),
                                String.valueOf(recordFields.get(keyStr)), fieldToSearch.get(keyStr).toString());
                        if (((boolean) searchResult.get("status"))) {
                            recordFields.put(keyStr, searchResult.get("crmid"));
                        }
                    }
                }
            }
            record.put("element", Util.getJson(recordFields));
            //System.out.println("Util.getJson(d) for Child Record = " + record);
            Object d = wsClient.doInvoke(Util.methodUPSERT, record, "POST");
            //System.out.println("Util.getJson(d) for Child Record = " + Util.getJson(d));
        }

    }

    private Map<String, Object> searchByID(Object response, String id) throws ParseException {
        Map<String, Object> objValue = new HashMap<>();
        JSONParser parser = new JSONParser();
        JSONArray resArray = (JSONArray) parser.parse(response.toString());
        for (Object object: resArray
             ) {
            JSONObject record = (JSONObject) parser.parse(object.toString());
            if (record.get("id") == id) {
                objValue = record;
            }
        }

        return objValue;
    }
    private static String encodeValue(Object value) {
        try {
            return URLEncoder.encode(String.valueOf(value), StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex.getCause());
        }
    }

    private Object doGet(String token, String _endpoint, String key) {
        Map<String, String> mapToSend = new HashMap<>();
        Header[] headersArray = new Header[2];
        headersArray[0] = new BasicHeader("Content-type", "application/json");
        headersArray[1] = new BasicHeader("Authorization", token);
        //System.out.println(Arrays.toString(headersArray));
        Object response = restClient.doGet(_endpoint, mapToSend, headersArray,key);
        if (response == null)
            return null;
        return response;
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
        //System.out.println("Util.getJson(d) = " + Util.getJson(d));
    }

    private Object getRecord(String module, String object) {
        String modulesIdField = modulesDeclared.getFieldsDoQuery(module).get(0);
        String condition = modulesIdField + "='" + object + "'";
        String query = "Select * from " + module + " where " + condition;
        Object response = wsClient.doQuery(query);
        return response;
    }

    private boolean startRestService() throws Exception {
        // Starting Rest Service
        this.restClient = new RESTClient(rest_api_url);
        String auth_credentials = "{\"username\": \""+username+"\", \"password\": \""+password+"\"}";
        if (!restClient.doAuthorization(auth_credentials, auth_endpoint)) {
            throw new Exception("Authorization Error");
        }
        return true;
    }
}