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
import java.text.SimpleDateFormat;
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
    RebalanceListner rebalanceListner;

    public UpdateConsumer() throws Exception {
        List topics = new ArrayList();
        topics.add(topic);
        rebalanceListner = new RebalanceListner(kafkaConsumer);
        kafkaConsumer.subscribe(topics, rebalanceListner);
    }

    public void init() {

        try {
            while (true) {
                ConsumerRecords records = kafkaConsumer.poll(Duration.ofMillis(3000));
                for (Object o : records) {
                    ConsumerRecord record = (ConsumerRecord) o;
                    readRecord(record);
                    rebalanceListner.setCurrentOffsets(record.topic(), record.partition(), record.offset());
                }
                kafkaConsumer.commitSync(rebalanceListner.getCurrentOffsets());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            // kafkaConsumer.close();
        }
    }


    private void readRecord(ConsumerRecord record) throws Exception {
        System.out.println(String.format("Topic - %s, Key - %s, Partition - %d, Value: %s", record.topic(), record.key(),record.partition(), record.value()));
        KeyData keyData = Util.getObjectFromJson((String) record.key(), KeyData.class);
        Object value = Util.getObjectFromJson((String) record.value(), Object.class);
        if (Objects.requireNonNull(keyData).operation.equals(Util.methodUPDATE)) {
            System.out.println("Upserting the Record");
            lastRecordToCreate.clear();
            //upsertRecord(keyData.module, (Map) value);
            if (!keyData.module.equals("ProcessLog")) {
                upsertRecord(keyData.module, (Map) value);
            } else {
                updateShipmentsStatus(keyData.module, (Map) value);
            }
        } else if (keyData.operation.equals(Util.methodDELETE)) {
            System.out.println("Deleting the Record");
            deleteRecord(keyData.module, (String) value);
        }
    }

    private void updateShipmentsStatus(String module, Map message) throws Exception{
        Map<String, Object> status = message;
        Map<String, Object> mapToSend = new HashMap<>();
        Map<String, Object> fieldUpdate = new HashMap<>();
        JSONObject processedMessageData = new JSONObject();
        StringBuilder queryCondition = new StringBuilder();
        if (!status.keySet().isEmpty() && (!status.values().isEmpty())) {
            for (Map.Entry<String, Object> entry : status.entrySet()) {
                String k = entry.getKey();
                if (entry.getValue() != null) {
                    /*
                    * The script should query ProcessLog module with the following conditions:
                    * */

                    String v = entry.getValue().toString();
                    String[] statusArray = v.split("#");
                    String statusLatestDate = "";
                    String latestStatus = "";
                    String linkToStatusCRMID = "";
                    String latestStatusShipmentKey = "";
                    for (String statusChanges:statusArray) {
                        String[] currentStatusArray = statusChanges.split("!");

                        /*
                         *   query Shipments module in order to find the record where pckslip_code == key. Connect ProcessLog to that
                         *   Shipment by filling linktoshipments with shipmentsid of the found record.
                         * */
                        Map<String, Object> searchShipment = searchRecord("Shipments", k,
                                "pckslip_code", "", false);
                        if (((boolean) searchShipment.get("status"))) {
                            queryCondition.setLength(0);
                            processedMessageData.put("linktoshipments", searchShipment.get("crmid"));
                            // queryCondition.append("linktoshipments ='").append(processedMessageData.get("linktoshipments")).append("'");
                        }

                        /*
                         * query Packages module in order to find the record where packagesrcid == 1st param value. Connect ProcessLog
                         * to that Package by filling linktopackages with packagesid of the found record.
                         * */
                        Map<String, Object> searchPackages = searchRecord("Packages", currentStatusArray[0],
                                "packagesrcid", "", false);
                        if (((boolean) searchPackages.get("status"))) {
                            processedMessageData.put("linktopackages", searchPackages.get("crmid"));
                            // queryCondition.append(" AND linktopackages ='").append(processedMessageData.get("linktopackages")).append("'");
                            queryCondition.append("linktopackages ='").append(processedMessageData.get("linktopackages")).append("'");
                        }


                        /*
                         * query cbStatus module in order to find the record where statussrcid == 4th param value.
                         * Connect ProcessLog to that cbStatus by filling its linktostatus with statusid of the found record
                         * */
                        Map<String, Object> searchcbStatus = searchRecord("cbStatus", currentStatusArray[3],
                                "statussrcid", "", false);
                        if (((boolean) searchcbStatus.get("status"))) {
                            processedMessageData.put("linktostatus", searchcbStatus.get("crmid"));
                            if (queryCondition.length() > 0) {
                                queryCondition.append(" AND linktostatus ='").append(processedMessageData.get("linktostatus")).append("'");
                            } else {
                                queryCondition.append("linktostatus ='").append(processedMessageData.get("linktostatus")).append("'");
                            }

                        }


                        /*
                         * dtime
                         * */
                        processedMessageData.put("dtime", currentStatusArray[2]);
                        if (queryCondition.length() > 0) {
                            queryCondition.append(" AND dtime ='").append(processedMessageData.get("dtime")).append("'");
                        } else {
                            queryCondition.append("dtime ='").append(processedMessageData.get("dtime")).append("'");
                        }
                        if (statusLatestDate.isEmpty()) {
                            statusLatestDate = processedMessageData.get("dtime").toString();
                            latestStatus = statusChanges;
                            linkToStatusCRMID = processedMessageData.get("linktostatus").toString();
                            latestStatusShipmentKey = k;
                        } else {
                            // we need to compare the dates
                            SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                            Date dtLatest = sdformat.parse(statusLatestDate);
                            Date dtToCompare= sdformat.parse(processedMessageData.get("dtime").toString());

                            if (dtLatest.compareTo(dtToCompare) < 0) {
                                statusLatestDate = processedMessageData.get("dtime").toString();
                                latestStatus = statusChanges;
                                linkToStatusCRMID = processedMessageData.get("linktostatus").toString();
                                latestStatusShipmentKey = k;
                            }
                        }

                        /*
                         * query cbCompany module in order to find the record where branchcode == 5th param value.
                         * Connect ProcessLog to that cbCompany by filling its linktomainbranch with cbcompanyid of the found record.
                         * */
                        Map<String, Object> searchcbCompany;
                        searchcbCompany = searchRecord("cbCompany", currentStatusArray[4],
                                "branchcode", "", false);
                        if (((boolean) searchcbCompany.get("status"))) {
                            processedMessageData.put("linktomainbranch", searchcbCompany.get("crmid"));
                            // queryCondition.append(" AND linktomainbranch ='").append(processedMessageData.get("linktomainbranch")).append("'");
                        }

                        /*
                         * query cbCompany module in order to find the record where branchcode == 6th param value.
                         * Connect ProcessLog to that cbCompany by filling its linktodestbranch with cbcompanyid of the found record.
                         * */
                        searchcbCompany = searchRecord("cbCompany", currentStatusArray[5],
                                "branchcode", "", false);
                        if (((boolean) searchcbCompany.get("status"))) {
                            processedMessageData.put("linktodestbranch", searchcbCompany.get("crmid"));
                            // queryCondition.append(" AND linktodestbranch ='").append(processedMessageData.get("linktodestbranch")).append("'");
                        }

                        /*String queryCondition = "linktoshipments ='" + processedMessageData.get("linktoshipments") + "'" +
                                " AND linktopackages ='" + processedMessageData.get("linktopackages") + "'" + " AND dtime ='" +
                                processedMessageData.get("dtime") + "'" + " AND linktostatus ='" +
                                processedMessageData.get("linktostatus") + "'" + " AND linktomainbranch ='" +
                                processedMessageData.get("linktomainbranch") + "'" + " AND linktodestbranch ='" +
                                processedMessageData.get("linktodestbranch") + "'";*/

                        Map<String, Object> searchProcessLog = searchRecord(module, "", "",
                                queryCondition.toString(), false);
                        if (!((boolean) searchProcessLog.get("status"))) {
                            // System.out.println("NDANIIIIIIIIIII" + processedMessageData);
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
                                if (processedMessageData.get(((JSONObject)field).get("fieldname").toString()) != null) {
                                    fieldUpdate.put(((JSONObject)field).get("fieldname").toString(), processedMessageData.get((
                                            (JSONObject)field).get("fieldname").toString()));
                                }
                            }
                            fieldUpdate.put("assigned_user_id", wsClient.getUserID());
                            mapToSend.put("elementType", module);
                            mapToSend.put("element", Util.getJson(fieldUpdate));
                            mapToSend.put("searchOn", "linktoshipments");
                            StringBuilder builderRemoveIndexZero = new StringBuilder(fieldUpdate.keySet().toString());
                            builderRemoveIndexZero.deleteCharAt(0);
                            StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                            builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                            String updatedfields = builderRemoveIndexLast.toString();
                            mapToSend.put("updatedfields", updatedfields);
                            // System.out.println("Map to Send" +  mapToSend);
                            Object d = wsClient.doInvoke(Util.methodUPSERT, mapToSend, "POST");
                            System.out.println("Util.getJson(d) = " + Util.getJson(d));
                            System.out.println("Updated Shipment Status");

                            /*
                            * http://phabricator.studioevolutivo.it/T10534#
                            * Packages
                            * */
                            // String[] latestStatusData = latestStatus.split("!");
                            // Map<String, Object> packageModuleFieldToUpdate = new HashMap<>();
                            // packageModuleFieldToUpdate.put("packagesrcid", latestStatusData[0]); // it will used as the search field too
                            // if (!linkToStatusCRMID.isEmpty())
                                // packageModuleFieldToUpdate.put("linktostatus", linkToStatusCRMID);
                            // packageModuleFieldToUpdate.put("statusdate", statusLatestDate);
                            // updateModuleRecord("Packages", "packagesrcid", packageModuleFieldToUpdate);

                            /*
                             * http://phabricator.studioevolutivo.it/T10534#
                             * Shipments
                             * */

                            // Map<String, Object> shipmentModuleFieldToUpdate = new HashMap<>();
                            // shipmentModuleFieldToUpdate.put("pckslip_code", latestStatusShipmentKey); // it will used as the search field too
                            // if (!linkToStatusCRMID.isEmpty())
                                // shipmentModuleFieldToUpdate.put("linktostatus", linkToStatusCRMID);
                            // shipmentModuleFieldToUpdate.put("statusdate", statusLatestDate);
                            // updateModuleRecord("Shipments", "pckslip_code", shipmentModuleFieldToUpdate);

                        }
                    }
                }
            }
        }
    }

    private void updateModuleRecord(String module, String searchOnField, Map<String, Object> fieldUpdate) {
        Map<String, Object> mapToSend = new HashMap<>();
        fieldUpdate.put("assigned_user_id", wsClient.getUserID());
        mapToSend.put("elementType", module);
        mapToSend.put("element", Util.getJson(fieldUpdate));
        mapToSend.put("searchOn", searchOnField);
        StringBuilder builderRemoveIndexZero = new StringBuilder(fieldUpdate.keySet().toString());
        builderRemoveIndexZero.deleteCharAt(0);
        StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
        builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
        String updatedfields = builderRemoveIndexLast.toString();
        mapToSend.put("updatedfields", updatedfields);
        System.out.println("Map to Send" +  mapToSend);
        Object d = wsClient.doInvoke(Util.methodUPSERT, mapToSend, "POST");
        System.out.println("Util.getJson(d) = " + Util.getJson(d));
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
            StringBuilder builderRemoveIndexZero = new StringBuilder(fieldUpdate.keySet().toString());
            builderRemoveIndexZero.deleteCharAt(0);
            StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
            builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
            String updatedfields = builderRemoveIndexLast.toString();
            mapToSend.put("updatedfields", updatedfields);
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

        System.out.println("Map to Send" +  mapToSend);

        Object d = wsClient.doInvoke(Util.methodUPSERT, mapToSend, "POST");
        System.out.println("Util.getJson(d) = " + Util.getJson(d));
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
        // System.out.println(orgfieldName);
        // System.out.println(fieldname);
        if(record.containsKey(orgfieldName) || orgfieldName.equals("distribuzioneFornitoreId") ||
                orgfieldName.equals("raeeFornitoreId")) {
            /*
            1. Check if the field value can be converted to JSONArray or JSONObject
            2. If is Object or JSON Array check if it exit in Module field search and is one of the module field
            3. if the above statement if false means the JSONArray or JSONObject record depend on the Main Module Record which is about to be created
            */
            String jsonValue = Util.getJson(record.get(orgfieldName));
            // System.out.println(jsonValue);
            // System.out.println(orgfieldName);
            JSONParser parser = new JSONParser();
            // System.out.println((parser.parse(jsonValue) instanceof JSONObject));
            if ((parser.parse(jsonValue) instanceof JSONObject) ||  orgfieldName.equals("distribuzioneFornitoreId") ||
                    orgfieldName.equals("raeeFornitoreId")) {
                // Get the Search fields
                 Map<String, Object> fieldToSearch = getSearchField(parentModule);
                // System.out.println(fieldToSearch);
                // System.out.println(moduleFieldInfo);
                 if (!fieldToSearch.isEmpty() && moduleFieldInfo.containsKey(fieldname)) {
                     String searchID;
                     if (orgfieldName.equals("distribuzioneFornitoreId") || orgfieldName.equals("raeeFornitoreId")) {
                         JSONObject importoSpedizione = (JSONObject) parser.parse(Util.getJson(record.get("importoSpedizione")));
                         // System.out.println(importoSpedizione.isEmpty());
                         // System.out.println(importoSpedizione);
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
                     //System.out.println(searchID);
                     Map<String, Object> searchResult = searchRecord(moduleFieldInfo.get(fieldname),
                             searchID, fieldToSearch.get(orgfieldName).toString(), "", true);
                     if (((boolean) searchResult.get("status")) && !((boolean) searchResult.get("mustbeupdated"))) {
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
                                         // if (moduleDateFields.containsKey(fieldname) && (moduleDateFields.get(fieldname).equals("5") || moduleDateFields.get(fieldname).equals("50"))) {
                                             // String dateValue = fonitoriObject.get(originalFiled.get("OrgfieldName")).toString().replace("T", " ");
                                             // recordField.put(((JSONObject)field).get("fieldname").toString(), dateValue);
                                         // } else {
                                             // assert fonitoriObject != null;
                                             recordField.put(((JSONObject)field).get("fieldname").toString(), fonitoriObject.get(originalFiled.get("OrgfieldName").toString()));
                                         // }
                                     }

                                     recordField.put("type", "Fornitore");
                                     recordField.put("assigned_user_id", wsClient.getUserID());
                                     recordMap.put("elementType", moduleFieldInfo.get(fieldname));
                                     recordMap.put("element", Util.getJson(recordField));
                                     recordMap.put("searchOn", fieldToSearch.get(orgfieldName));
                                     StringBuilder builderRemoveIndexZero = new StringBuilder(recordField.keySet().toString());
                                     builderRemoveIndexZero.deleteCharAt(0);
                                     StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                     builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                     String updatedfields = builderRemoveIndexLast.toString();
                                     recordMap.put("updatedfields", updatedfields);
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
                             // 1. Get Map for Adding that Module from Rest API
                             String mapName = orgfieldName + "2" + moduleFieldInfo.get(fieldname);
                             // System.out.println(mapName);
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
                                 //System.out.println("Before calling getFieldValue");
                                 Object value = getFieldValue(originalFiled.get("OrgfieldName").toString(), (Map) record.get(orgfieldName), moduleFieldInfo, parentModule, fieldname);
                                 //System.out.println(value);
                                 if (((JSONObject) value).get("status").toString().equals("found")) {
                                     //System.out.println(field);
                                     if (moduleDateFields.containsKey(fieldname) &&
                                             (moduleDateFields.get(fieldname).equals("5") ||
                                                     moduleDateFields.get(fieldname).equals("50"))) {
                                         String dateValue = ((JSONObject)value).get("value").toString().replace("T", " ");
                                         recordField.put(((JSONObject)field).get("fieldname").toString(), dateValue);
                                     } else {
                                         recordField.put(((JSONObject)field).get("fieldname").toString(), ((JSONObject)value).get("value"));
                                     }
                                     //System.out.println(recordField);
                                 }
                             }


                             // Handle Special Case for indirizzoMittente, indirizzoRitiro, indirizzoConsegna, indirizzoDestinatario
                             if (orgfieldName.equals("indirizzoMittente") || orgfieldName.equals("indirizzoRitiro") ||
                                     orgfieldName.equals("indirizzoConsegna") || orgfieldName.equals("indirizzoDestinatario")) {
                                 /*
                                  * Query Geoboundary Module  where geoname == comune
                                  */
                                 Map<String, Object> searchResultGeoboundary = searchRecord("Geoboundary",
                                         ((JSONObject) parser.parse(jsonValue)).get("comune").toString(),
                                         "geoname", "", false);
                                 /*
                                 * Otherwise, link the cbAddress with the GeoBoundary record where geoname == DA VERIFICARE
                                 * */

                                 Map<String, Object> searchResultGeoboundaryDefault = searchRecord("Geoboundary",
                                         "DA VERIFICARE", "geoname", "", false);
                                 if (((boolean) searchResultGeoboundary.get("status")) || ((boolean) searchResultGeoboundaryDefault.get("status"))) {
                                     Map<String, String> referenceFields = getUIType10Field(moduleFieldInfo.get(fieldname));
                                     for (Object key : referenceFields.keySet()) {
                                         String keyStr = (String)key;
                                         if (referenceFields.get(keyStr).equals("GeoBoundary")) {
                                             if (((boolean) searchResultGeoboundary.get("status"))) {
                                                 recordField.put(keyStr, searchResultGeoboundary.get("crmid"));
                                             } else {
                                                 recordField.put(keyStr, searchResultGeoboundaryDefault.get("crmid"));
                                             }
                                         }
                                     }

                                 }
                             }

                             System.out.println("RITIRO PROCESSING START");
                             if (orgfieldName.equals("ritiro")) {
                                 System.out.println("RITIRO PROCESSSING ENTER");
                                 /*
                                  * Query cbCompany module in order to check whether there already exists a record where branchsrcid == filialeId.
                                  */
                                 Map<String, Object> searchResultCompany = searchRecord("cbCompany",
                                         ((JSONObject) parser.parse(jsonValue)).get("filialeId").toString(),
                                         "branchsrcid", "", true);

                                 System.out.println("RITIRO");
                                 if (((boolean) searchResultCompany.get("status")) && !((boolean) searchResultCompany.get("mustbeupdated"))) {
                                     Map<String, String> referenceFields = getUIType10Field(moduleFieldInfo.get(fieldname));
                                     System.out.println("MOduleeee::" + moduleFieldInfo.get(fieldname));
                                     System.out.println("Reference Field Company::" + referenceFields);
                                     for (Object key : referenceFields.keySet()) {
                                         String keyStr = (String)key;
                                        // System.out.println("Key String::" + keyStr);
                                         if (referenceFields.get(keyStr).equals("cbCompany")) {
                                             //System.out.println("value::" + searchResultCompany.get("crmid"));
                                             recordField.put(keyStr, searchResultCompany.get("crmid"));
                                             System.out.println("Record Field::" + recordField);
                                         }
                                     }

                                 } else {
                                     /*
                                      * Query cbCompany module in order to check whether there already exists a record where branchsrcid == filialeId.
                                      */
                                     System.out.println("CREATING RITIRO");
                                     //System.out.println(parser.parse(jsonValue));
                                     if (startRestService()) {
                                         if (parser.parse(jsonValue) != null) {
                                             String endpoint = "filiali";
                                             String objectKey = "filiali";
                                             JSONObject ritiro = (JSONObject) parser.parse(jsonValue);
                                             System.out.println(ritiro);
                                             String id = ritiro.get("filialeId").toString();
                                             System.out.println(id);

                                             Object filialiResponse = doGet(restClient.get_servicetoken(), endpoint, objectKey);
                                             System.out.println(filialiResponse);
                                             if (filialiResponse != null) {
                                                 System.out.println("GETTING FILIALI");
                                                 Map<String, Object> filialiObject = searchByID(filialiResponse, id);
                                                 System.out.println(filialiObject);
                                                 if (!filialiObject.isEmpty()) {
                                                     Map<String, Object> recordMapFiliali = new HashMap<>();
                                                     Map<String, Object> recordFieldFiliali = new HashMap<>();
                                                     String mapNameFiliali = "filialeId2cbCompany";
                                                     String mapModuleFiliali = "cbMap";
                                                     String conditionFiliali = "mapname" + "='" + mapNameFiliali + "'";
                                                     String queryMapFiliali = "select * from " + mapModuleFiliali + " where " + conditionFiliali;
                                                     JSONArray mapdataFiliali = wsClient.doQuery(queryMapFiliali);
                                                     JSONObject resultFiliali = (JSONObject)parser.parse(mapdataFiliali.get(0).toString());
                                                     JSONObject contentjsonFiliali = (JSONObject)parser.parse(resultFiliali.get("contentjson").toString());
                                                     JSONObject fieldsFiliali = (JSONObject)parser.parse(contentjsonFiliali.get("fields").toString());
                                                     JSONArray fields_arrayFiliali = (JSONArray) fieldsFiliali.get("field");
                                                     for (Object field: fields_arrayFiliali) {
                                                         JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                                         JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                                         recordFieldFiliali.put(((JSONObject)field).get("fieldname").toString(), filialiObject.get(originalFiled.get("OrgfieldName").toString()));
                                                     }
                                                     System.out.println("RECORD FILIALI");
                                                     System.out.println(recordFieldFiliali);

                                                     /*
                                                     * Query GeoBoundary module and find the record where geoname == comune parameter of the API output.
                                                     * Store in geobid field of the new cbCompany the value of geobid of the found GeoBoundary record
                                                     * */
                                                     if (((JSONObject) parser.parse(filialiObject.toString())).get("comune") != null &&
                                                             !((JSONObject) parser.parse(filialiObject.toString())).get("comune").toString().isEmpty()) {
                                                         Map<String, Object> searchResultGeoboundary = searchRecord("Geoboundary",
                                                                 ((JSONObject) parser.parse(filialiObject.toString())).get("comune").toString(),
                                                                 "geoname", "", false);
                                                         System.out.println("MCHAWI");
                                                         System.out.println(searchResultGeoboundary);
                                                         /*
                                                         * Otherwise, link the cbAddress with the GeoBoundary record where geoname == DA VERIFICARE
                                                         * */
                                                         Map<String, Object> searchResultGeoboundaryDefault = searchRecord(
                                                                 "Geoboundary", "DA VERIFICARE", "geoname",
                                                                 "", false);

                                                        if (((boolean) searchResultGeoboundary.get("status")) || ((boolean) searchResultGeoboundaryDefault.get("status"))) {
                                                         Map<String, String> referenceFields = getUIType10Field("cbCompany");
                                                         for (Object key : referenceFields.keySet()) {
                                                             String keyStr = (String)key;
                                                             if (referenceFields.get(keyStr).equals("GeoBoundary")) {
                                                                 if (((boolean) searchResultGeoboundary.get("status"))) {
                                                                     recordFieldFiliali.put(keyStr, searchResultGeoboundary.get("crmid"));
                                                                 } else {
                                                                     recordFieldFiliali.put(keyStr, searchResultGeoboundaryDefault.get("crmid"));
                                                                 }

                                                             }
                                                         }
                                                     }
                                                     } else {
                                                         /*
                                                          * Otherwise, link the cbAddress with the GeoBoundary record where geoname == DA VERIFICARE
                                                          * */
                                                         Map<String, Object> searchResultGeoboundaryDefault = searchRecord(
                                                                 "Geoboundary", "DA VERIFICARE", "geoname",
                                                                 "", false);
                                                         if (((boolean) searchResultGeoboundaryDefault.get("status"))) {
                                                             Map<String, String> referenceFields = getUIType10Field("cbCompany");
                                                             for (Object key : referenceFields.keySet()) {
                                                                 String keyStr = (String)key;
                                                                 if (referenceFields.get(keyStr).equals("GeoBoundary")) {
                                                                     recordFieldFiliali.put(keyStr, searchResultGeoboundaryDefault.get("crmid"));
                                                                 }
                                                             }
                                                         }
                                                     }

                                                     System.out.println("MAMAMAMAMAM VVVVVVVVVVVVV");
                                                     Map<String, Object> searchResultVendorModule;
                                                     /*
                                                     * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                                                     * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                                                     * vettoreId
                                                     * */
                                                     System.out.println(((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString());
                                                     searchResultVendorModule = searchRecord("Vendors",
                                                     ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString(),
                                                             "suppliersrcid", "Vettore", true);

                                                     //System.out.println(searchResultGeoboundary);
                                                     if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                                         recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                                     } else {
                                                         // To Search in Rest Service
                                                         if (startRestService()) {
                                                             String vettoriEndpoint = "vettori";
                                                             String vettoriDataKey = "vettori";

                                                             Object vettoriResponse = doGet(restClient.get_servicetoken(), vettoriEndpoint, vettoriDataKey);
                                                             //System.out.println(vettoriResponse);
                                                             //System.out.println(filialiResponse);
                                                             if (vettoriResponse != null) {
                                                                 Map<String, Object> vettoriObject = searchByID(vettoriResponse,
                                                                         ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString());
                                                                 if (!vettoriObject.isEmpty()) {
                                                                     Map<String, Object> vettoriRecordMap = new HashMap<>();
                                                                     Map<String, Object> vettoriRecordField = new HashMap<>();
                                                                     //String vettoriMapName = orgfieldName + "2" + fieldname;
                                                                     String vettoriMapName = "vettoreId2Vendors";
                                                                     String vettoriMapModule = "cbMap";
                                                                     String vettoriCondition = "mapname" + "='" + vettoriMapName + "'";
                                                                     String vettoriQueryMap = "select * from " + vettoriMapModule + " where " + vettoriCondition;
                                                                     JSONArray vettoriMapData = wsClient.doQuery(vettoriQueryMap);
                                                                     JSONObject vettoriQueryResult = (JSONObject)parser.parse(vettoriMapData.get(0).toString());
                                                                     JSONObject vettoriMapContentJSON = (JSONObject)parser.parse(vettoriQueryResult.get("contentjson").toString());
                                                                     JSONObject vettoriMapFields = (JSONObject)parser.parse(vettoriMapContentJSON.get("fields").toString());
                                                                     JSONArray vettoriFieldsArray = (JSONArray) vettoriMapFields.get("field");
                                                                     for (Object field: vettoriFieldsArray) {
                                                                         JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                                                         JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                                                         vettoriRecordField.put(((JSONObject)field).get("fieldname").toString(), vettoriObject.get(originalFiled.get("OrgfieldName").toString()));
                                                                     }

                                                                     vettoriRecordField.put("assigned_user_id", wsClient.getUserID());
                                                                     vettoriRecordField.put("type", "Vettore");
                                                                     vettoriRecordMap.put("elementType", "Vendors");
                                                                     vettoriRecordMap.put("element", Util.getJson(vettoriRecordField));
                                                                     vettoriRecordMap.put("searchOn", "suppliersrcid");
                                                                     StringBuilder builderRemoveIndexZero = new StringBuilder(vettoriRecordField.keySet().toString());
                                                                     builderRemoveIndexZero.deleteCharAt(0);
                                                                     StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                                                     builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                                                     String updatedfields = builderRemoveIndexLast.toString();
                                                                     vettoriRecordMap.put("updatedfields", updatedfields);
                                                                     Object newRecord = wsClient.doInvoke(Util.methodUPSERT, vettoriRecordMap, "POST");
                                                                     JSONObject vettoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                                     if (vettoriobjrec.containsKey("id") && !vettoriobjrec.get("id").toString().equals("")) {
                                                                         recordFieldFiliali.put("linktocarrier", vettoriobjrec.get("id").toString());
                                                                     }
                                                                 }
                                                             }
                                                         }
                                                     }

                                                     /*
                                                      * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                                                      * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                                                      * fornitoreId
                                                      * */
                                                     searchResultVendorModule = searchRecord("Vendors",
                                                             ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString(),
                                                             "suppliersrcid", "Fornitore", true);
                                                     if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                                         recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                                     } else {
                                                         // To Search in Rest Service
                                                         if (startRestService()) {
                                                             String fornitoriEndpoint = "fornitori";
                                                             String fornitoriDataKey = "fornitori";

                                                             Object fornitoriResponse = doGet(restClient.get_servicetoken(), fornitoriEndpoint, fornitoriDataKey);
                                                             if (fornitoriResponse != null) {
                                                                 Map<String, Object> fornitoriObject = searchByID(fornitoriResponse,
                                                                         ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString());
                                                                 if (!fornitoriObject.isEmpty()) {
                                                                     Map<String, Object> fornitoriRecordMap = new HashMap<>();
                                                                     Map<String, Object> fornitoriRecordField = new HashMap<>();
                                                                     //String fornitoriMapName = orgfieldName + "2" + fieldname;
                                                                     String fornitoriMapName = "fornitoreId2Vendors";
                                                                     String fornitoriMapModule = "cbMap";
                                                                     String fornitoriCondition = "mapname" + "='" + fornitoriMapName + "'";
                                                                     String fornitoriQueryMap = "select * from " + fornitoriMapModule + " where " + fornitoriCondition;
                                                                     JSONArray fornitoriMapData = wsClient.doQuery(fornitoriQueryMap);
                                                                     JSONObject fornitoriQueryResult = (JSONObject)parser.parse(fornitoriMapData.get(0).toString());
                                                                     JSONObject fornitoriMapContentJSON = (JSONObject)parser.parse(fornitoriQueryResult.get("contentjson").toString());
                                                                     JSONObject fornitoriMapFields = (JSONObject)parser.parse(fornitoriMapContentJSON.get("fields").toString());
                                                                     JSONArray fornitoriFieldsArray = (JSONArray) fornitoriMapFields.get("field");
                                                                     for (Object field: fornitoriFieldsArray) {
                                                                         JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                                                         JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                                                         fornitoriRecordField.put(((JSONObject)field).get("fieldname").toString(), fornitoriObject.get(originalFiled.get("OrgfieldName").toString()));
                                                                     }

                                                                     fornitoriRecordField.put("assigned_user_id", wsClient.getUserID());
                                                                     fornitoriRecordField.put("type", "Vettore");
                                                                     fornitoriRecordMap.put("elementType", "Vendors");
                                                                     fornitoriRecordMap.put("element", Util.getJson(fornitoriRecordField));
                                                                     fornitoriRecordMap.put("searchOn", "suppliersrcid");
                                                                     StringBuilder builderRemoveIndexZero = new StringBuilder(fornitoriRecordField.keySet().toString());
                                                                     builderRemoveIndexZero.deleteCharAt(0);
                                                                     StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                                                     builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                                                     String updatedfields = builderRemoveIndexLast.toString();
                                                                     fornitoriRecordMap.put("updatedfields", updatedfields);
                                                                     Object newRecord = wsClient.doInvoke(Util.methodUPSERT, fornitoriRecordMap, "POST");
                                                                     JSONObject fornitoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                                     if (fornitoriobjrec.containsKey("id") && !fornitoriobjrec.get("id").toString().equals("")) {
                                                                         recordFieldFiliali.put("vendorid", fornitoriobjrec.get("id").toString());
                                                                     }
                                                                 }
                                                             }
                                                         }
                                                     }

                                                     recordFieldFiliali.put("assigned_user_id", wsClient.getUserID());
                                                     recordMapFiliali.put("elementType", "cbCompany");
                                                     recordMapFiliali.put("element", Util.getJson(recordFieldFiliali));
                                                     recordMapFiliali.put("searchOn", "branchsrcid");
                                                     StringBuilder builderRemoveIndexZero = new StringBuilder(recordFieldFiliali.keySet().toString());
                                                     builderRemoveIndexZero.deleteCharAt(0);
                                                     StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                                     builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                                     String updatedfields = builderRemoveIndexLast.toString();
                                                     recordMapFiliali.put("updatedfields", updatedfields);
                                                     System.out.println(recordMapFiliali);
                                                     Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapFiliali, "POST");
                                                     JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                     if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                                                         recordField.put("branchid", obj.get("id").toString());
                                                     }
                                                 }
                                             }
                                         }
                                     }
                                 }

                                 /*
                                  * FOR NEW REQUIREMENT USE EXITING CODE
                                  * http://phabricator.studioevolutivo.it/T10781
                                  * 	Query GeoBoundary module in order to find the record where geoname == comune.
                                  * If the query returns a result, then relate Pickups with that GeoBoundary by filling its linktocities field with geobid.
                                  *  If there exists none, then implement the solution described here
                                  * */

                                 if (((JSONObject) parser.parse(jsonValue)).get("comune") != null &&
                                         !((JSONObject) parser.parse(jsonValue)).get("comune").toString().isEmpty()) {
                                     Map<String, Object> searchResultGeoboundary = searchRecord("Geoboundary",
                                             ((JSONObject) parser.parse(jsonValue)).get("comune").toString(),
                                             "geoname", "", false);


                                     Map<String, Object> searchResultGeoboundaryDefault = searchRecord(
                                             "Geoboundary", "DA VERIFICARE", "geoname",
                                             "", false);

                                     if (((boolean) searchResultGeoboundary.get("status")) || ((boolean) searchResultGeoboundaryDefault.get("status"))) {
                                         Map<String, String> referenceFields = getUIType10Field("Pickups");
                                         for (Object key : referenceFields.keySet()) {
                                             String keyStr = (String)key;
                                             if (referenceFields.get(keyStr).equals("GeoBoundary")) {
                                                 if (((boolean) searchResultGeoboundary.get("status"))) {
                                                     recordField.put(keyStr, searchResultGeoboundary.get("crmid"));
                                                 } else {
                                                     recordField.put(keyStr, searchResultGeoboundaryDefault.get("crmid"));
                                                 }

                                             }
                                         }
                                     }
                                 } else {
                                     /*
                                      * Otherwise, link the cbAddress with the GeoBoundary record where geoname == DA VERIFICARE
                                      * */
                                     Map<String, Object> searchResultGeoboundaryDefault = searchRecord(
                                             "Geoboundary", "DA VERIFICARE", "geoname",
                                             "", false);
                                     if (((boolean) searchResultGeoboundaryDefault.get("status"))) {
                                         Map<String, String> referenceFields = getUIType10Field("Pickups");
                                         for (Object key : referenceFields.keySet()) {
                                             String keyStr = (String)key;
                                             if (referenceFields.get(keyStr).equals("GeoBoundary")) {
                                                 recordField.put(keyStr, searchResultGeoboundaryDefault.get("crmid"));
                                             }
                                         }
                                     }

                                 }

                             }

                             if (orgfieldName.equals("zonaConsegna")) {
                                 /*
                                  * Query DeliveryAreas module in order to check whether there already exists a record where areasrcid == zonaConsegna.ID.
                                  */
                                 System.out.println(parser.parse(jsonValue));
                                 Map<String, Object> searchResultDeliveryAreas = searchRecord("DeliveryAreas",
                                         ((JSONObject) parser.parse(jsonValue)).get("ID").toString(),
                                         "areasrcid", "", true);
                                 System.out.println(searchResultDeliveryAreas);

                                 if (((boolean) searchResultDeliveryAreas.get("status")) && !((boolean) searchResultDeliveryAreas.get("mustbeupdated"))) {
                                     Map<String, String> referenceFields = getUIType10Field(moduleFieldInfo.get(fieldname));
                                     for (Object key : referenceFields.keySet()) {
                                         String keyStr = (String)key;
                                         if (referenceFields.get(keyStr).equals("DeliveryAreas")) {
                                             recordField.put(keyStr, searchResultDeliveryAreas.get("crmid"));
                                         }
                                     }

                                 } else {
                                     /*
                                      * Query cbCompany module in order to check whether there already exists a record where branchsrcid == filialeId.
                                      */
                                     if (startRestService()) {
                                         if (parser.parse(jsonValue) != null) {
                                             String endpoint = "filiali";
                                             String objectKey = "filiali";
                                             //.out.println("DELIVERY AREEEEEEEEEEEEEEEEEEEEE");
                                             // System.out.println(parser.parse(jsonValue));
                                             JSONObject zonaConsegna = (JSONObject) parser.parse(jsonValue);
                                             String id = zonaConsegna.get("filialeId").toString();

                                             Object filialiResponse = doGet(restClient.get_servicetoken(), endpoint, objectKey);
                                             //System.out.println(filialiResponse);
                                             if (filialiResponse != null) {
                                                 Map<String, Object> filialiObject = searchByID(filialiResponse, id);
                                                 if (!filialiObject.isEmpty()) {
                                                     Map<String, Object> recordMapFiliali = new HashMap<>();
                                                     Map<String, Object> recordFieldFiliali = new HashMap<>();
                                                     String mapNameFiliali = "filialeId2cbCompany";
                                                     String mapModuleFiliali = "cbMap";
                                                     String conditionFiliali = "mapname" + "='" + mapNameFiliali + "'";
                                                     String queryMapFiliali = "select * from " + mapModuleFiliali + " where " + conditionFiliali;
                                                     JSONArray mapdataFiliali = wsClient.doQuery(queryMapFiliali);
                                                     JSONObject resultFiliali = (JSONObject)parser.parse(mapdataFiliali.get(0).toString());
                                                     JSONObject contentjsonFiliali = (JSONObject)parser.parse(resultFiliali.get("contentjson").toString());
                                                     JSONObject fieldsFiliali = (JSONObject)parser.parse(contentjsonFiliali.get("fields").toString());
                                                     JSONArray fields_arrayFiliali = (JSONArray) fieldsFiliali.get("field");
                                                     for (Object field: fields_arrayFiliali) {
                                                         JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                                         JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                                         recordFieldFiliali.put(((JSONObject)field).get("fieldname").toString(), filialiObject.get(originalFiled.get("OrgfieldName").toString()));
                                                     }

                                                     // System.out.println("RECORD DARA");
                                                     // System.out.println(recordFieldFiliali);
                                                     /*
                                                      * Query GeoBoundary module and find the record where geoname == comune parameter of the API output.
                                                      * Store in geobid field of the new cbCompany the value of geobid of the found GeoBoundary record
                                                      * */
                                                     if (filialiObject.get("comune") != null && !filialiObject.get("comune").toString().isEmpty()) {
                                                         Map<String, Object> searchResultGeoboundary = searchRecord(
                                                                 "Geoboundary", filialiObject.get("comune").toString(),
                                                                 "geoname", "", false);
                                                         Map<String, Object> searchResultGeoboundaryDefault = searchRecord("Geoboundary",
                                                                 "DA VERIFICARE", "geoname", "", false);
                                                         // System.out.println("SEARCH GEOBOUNDARY");
                                                         // System.out.println(searchResultGeoboundary);
                                                         if (((boolean) searchResultGeoboundary.get("status")) || ((boolean) searchResultGeoboundaryDefault.get("status"))) {
                                                             Map<String, String> referenceFields = getUIType10Field("cbCompany");
                                                             for (Object key : referenceFields.keySet()) {
                                                                 String keyStr = (String)key;
                                                                 if (referenceFields.get(keyStr).equals("GeoBoundary")) {
                                                                     if (((boolean) searchResultGeoboundary.get("status"))) {
                                                                         recordFieldFiliali.put(keyStr, searchResultGeoboundary.get("crmid"));
                                                                     } else {
                                                                         recordFieldFiliali.put(keyStr, searchResultGeoboundaryDefault.get("crmid"));
                                                                     }

                                                                 }
                                                             }
                                                         }
                                                     } else {
                                                         Map<String, Object> searchResultGeoboundaryDefault = searchRecord("Geoboundary",
                                                                 "DA VERIFICARE", "geoname", "", false);
                                                         if (((boolean) searchResultGeoboundaryDefault.get("status"))) {
                                                             Map<String, String> referenceFields = getUIType10Field("cbCompany");
                                                             for (Object key : referenceFields.keySet()) {
                                                                 String keyStr = (String)key;
                                                                 if (referenceFields.get(keyStr).equals("GeoBoundary")) {
                                                                         recordFieldFiliali.put(keyStr, searchResultGeoboundaryDefault.get("crmid"));
                                                                 }
                                                             }
                                                         }
                                                     }


                                                     Map<String, Object> searchResultVendorModule;

                                                     if (filialiObject.get("vettoreId") != null) {
                                                         /*
                                                          * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                                                          * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                                                          * vettoreId
                                                          * */
                                                         System.out.println(((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString());
                                                         searchResultVendorModule = searchRecord("Vendors",
                                                                 filialiObject.get("vettoreId").toString(),
                                                                 "suppliersrcid", "Vettore", true);

                                                         if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                                             recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                                         } else {
                                                             // To Search in Rest Service
                                                             if (startRestService()) {
                                                                 String vettoriEndpoint = "vettori";
                                                                 String vettoriDataKey = "vettori";

                                                                 Object vettoriResponse = doGet(restClient.get_servicetoken(), vettoriEndpoint, vettoriDataKey);
                                                                 // System.out.println(vettoriResponse);
                                                                 if (vettoriResponse != null) {
                                                                     Map<String, Object> vettoriObject = searchByID(vettoriResponse,
                                                                             ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString());
                                                                     if (!vettoriObject.isEmpty()) {
                                                                         Map<String, Object> vettoriRecordMap = new HashMap<>();
                                                                         Map<String, Object> vettoriRecordField = new HashMap<>();
                                                                         //String vettoriMapName = orgfieldName + "2" + fieldname;
                                                                         String vettoriMapName = "vettoreId2Vendors";
                                                                         String vettoriMapModule = "cbMap";
                                                                         String vettoriCondition = "mapname" + "='" + vettoriMapName + "'";
                                                                         String vettoriQueryMap = "select * from " + vettoriMapModule + " where " + vettoriCondition;
                                                                         JSONArray vettoriMapData = wsClient.doQuery(vettoriQueryMap);
                                                                         JSONObject vettoriQueryResult = (JSONObject)parser.parse(vettoriMapData.get(0).toString());
                                                                         JSONObject vettoriMapContentJSON = (JSONObject)parser.parse(vettoriQueryResult.get("contentjson").toString());
                                                                         JSONObject vettoriMapFields = (JSONObject)parser.parse(vettoriMapContentJSON.get("fields").toString());
                                                                         JSONArray vettoriFieldsArray = (JSONArray) vettoriMapFields.get("field");
                                                                         for (Object field: vettoriFieldsArray) {
                                                                             JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                                                             JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                                                             vettoriRecordField.put(((JSONObject)field).get("fieldname").toString(), vettoriObject.get(originalFiled.get("OrgfieldName").toString()));
                                                                         }

                                                                         vettoriRecordField.put("assigned_user_id", wsClient.getUserID());
                                                                         vettoriRecordField.put("type", "Vettore");
                                                                         vettoriRecordMap.put("elementType", "Vendors");
                                                                         vettoriRecordMap.put("element", Util.getJson(vettoriRecordField));
                                                                         vettoriRecordMap.put("searchOn", "suppliersrcid");
                                                                         StringBuilder builderRemoveIndexZero = new StringBuilder(vettoriRecordField.keySet().toString());
                                                                         builderRemoveIndexZero.deleteCharAt(0);
                                                                         StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                                                         builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                                                         String updatedfields = builderRemoveIndexLast.toString();
                                                                         vettoriRecordMap.put("updatedfields", updatedfields);
                                                                         Object newRecord = wsClient.doInvoke(Util.methodUPSERT, vettoriRecordMap, "POST");
                                                                         JSONObject vettoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                                         if (vettoriobjrec.containsKey("id") && !vettoriobjrec.get("id").toString().equals("")) {
                                                                             recordFieldFiliali.put("linktocarrier", vettoriobjrec.get("id").toString());
                                                                         }
                                                                     }
                                                                 }
                                                             }
                                                         }
                                                     }

                                                     if (filialiObject.get("fornitoreId") != null) {
                                                         /*
                                                          * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                                                          * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                                                          * fornitoreId
                                                          * */
                                                         searchResultVendorModule = searchRecord("Vendors",
                                                                 filialiObject.get("fornitoreId").toString(),
                                                                 "suppliersrcid", "Fornitore", true);
                                                         if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                                             recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                                         } else {
                                                             // To Search in Rest Service
                                                             if (startRestService()) {
                                                                 String fornitoriEndpoint = "fornitori";
                                                                 String fornitoriDataKey = "fornitori";

                                                                 Object fornitoriResponse = doGet(restClient.get_servicetoken(), fornitoriEndpoint, fornitoriDataKey);
                                                                 if (fornitoriResponse != null) {
                                                                     Map<String, Object> fornitoriObject = searchByID(fornitoriResponse,
                                                                             ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString());
                                                                     if (!fornitoriObject.isEmpty()) {
                                                                         Map<String, Object> fornitoriRecordMap = new HashMap<>();
                                                                         Map<String, Object> fornitoriRecordField = new HashMap<>();
                                                                         //String fornitoriMapName = orgfieldName + "2" + fieldname;
                                                                         String fornitoriMapName = "fornitoreId2Vendors";
                                                                         String fornitoriMapModule = "cbMap";
                                                                         String fornitoriCondition = "mapname" + "='" + fornitoriMapName + "'";
                                                                         String fornitoriQueryMap = "select * from " + fornitoriMapModule + " where " + fornitoriCondition;
                                                                         JSONArray fornitoriMapData = wsClient.doQuery(fornitoriQueryMap);
                                                                         JSONObject fornitoriQueryResult = (JSONObject)parser.parse(fornitoriMapData.get(0).toString());
                                                                         JSONObject fornitoriMapContentJSON = (JSONObject)parser.parse(fornitoriQueryResult.get("contentjson").toString());
                                                                         JSONObject fornitoriMapFields = (JSONObject)parser.parse(fornitoriMapContentJSON.get("fields").toString());
                                                                         JSONArray fornitoriFieldsArray = (JSONArray) fornitoriMapFields.get("field");
                                                                         for (Object field: fornitoriFieldsArray) {
                                                                             JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                                                             JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                                                             fornitoriRecordField.put(((JSONObject)field).get("fieldname").toString(), fornitoriObject.get(originalFiled.get("OrgfieldName").toString()));
                                                                         }

                                                                         fornitoriRecordField.put("assigned_user_id", wsClient.getUserID());
                                                                         fornitoriRecordField.put("type", "Vettore");
                                                                         fornitoriRecordMap.put("elementType", "Vendors");
                                                                         fornitoriRecordMap.put("element", Util.getJson(fornitoriRecordField));
                                                                         fornitoriRecordMap.put("searchOn", "suppliersrcid");
                                                                         StringBuilder builderRemoveIndexZero = new StringBuilder(fornitoriRecordField.keySet().toString());
                                                                         builderRemoveIndexZero.deleteCharAt(0);
                                                                         StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                                                         builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                                                         String updatedfields = builderRemoveIndexLast.toString();
                                                                         fornitoriRecordMap.put("updatedfields", updatedfields);
                                                                         Object newRecord = wsClient.doInvoke(Util.methodUPSERT, fornitoriRecordMap, "POST");
                                                                         JSONObject fornitoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                                         if (fornitoriobjrec.containsKey("id") && !fornitoriobjrec.get("id").toString().equals("")) {
                                                                             recordFieldFiliali.put("vendorid", fornitoriobjrec.get("id").toString());
                                                                         }
                                                                     }
                                                                 }
                                                             }
                                                         }
                                                     }

                                                     recordFieldFiliali.put("assigned_user_id", wsClient.getUserID());
                                                     recordMapFiliali.put("elementType", "cbCompany");
                                                     recordMapFiliali.put("element", Util.getJson(recordFieldFiliali));
                                                     recordMapFiliali.put("searchOn", "branchsrcid");
                                                     StringBuilder builderRemoveIndexZero = new StringBuilder(recordFieldFiliali.keySet().toString());
                                                     builderRemoveIndexZero.deleteCharAt(0);
                                                     StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                                     builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                                     String updatedfields = builderRemoveIndexLast.toString();
                                                     recordMapFiliali.put("updatedfields", updatedfields);
                                                     // System.out.println(recordMapFiliali);
                                                     Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapFiliali, "POST");
                                                     JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                     if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                                                         recordField.put("linktobranch", obj.get("id").toString());
                                                     }
                                                 }
                                             }
                                         }
                                     }

                                     /* In zonaConsegna, there is an API parameter, called tecnicoId.
                                        Query Technicians module in order to check whether there already exists a record where techniciansrcid == zonaConsegna.tecnicoId. If there exists none, then make an HTTP request to GET /tecnici/{id} where id should be the value of zonaConsegna.tecnicoId.
                                        Afterwards, create a new Technicians record in CoreBOS with the following mapping:
                                        */
                                     if (((JSONObject) parser.parse(jsonValue)).get("tecnicoId") != null) {
                                         Map<String, Object> searchResultTechnicians = searchRecord("Technicians",
                                                 ((JSONObject) parser.parse(jsonValue)).get("tecnicoId").toString(),
                                                 "techniciansrcid", "", true);

                                         if (((boolean) searchResultTechnicians.get("status")) && !((boolean) searchResultTechnicians.get("mustbeupdated"))) {
                                             Map<String, String> referenceFields = getUIType10Field(moduleFieldInfo.get(fieldname));
                                             for (Object key : referenceFields.keySet()) {
                                                 String keyStr = (String)key;
                                                 if (referenceFields.get(keyStr).equals("DeliveryAreas")) {
                                                     recordField.put(keyStr, searchResultTechnicians.get("crmid"));
                                                 }
                                             }

                                         } else {
                                             if (startRestService()) {
                                                 String endpoint = "tecnici";
                                                 String objectKey = "tecnico";
                                                 JSONObject zonaConsegna = (JSONObject) parser.parse(jsonValue);
                                                 String id = zonaConsegna.get("tecnicoId").toString();
                                                 Object tecniciResponse = doGet(restClient.get_servicetoken(),
                                                         endpoint+"/" + id, objectKey);

                                                 if (tecniciResponse != null) {
                                                     JSONObject tecniciObject = (JSONObject) parser.parse(tecniciResponse.toString());
                                                     JSONObject indirizzoObject = (JSONObject) tecniciObject.get("indirizzo");
                                                     Map<String, Object> recordMapTecnici = new HashMap<>();
                                                     Map<String, Object> recordFieldTecnici = new HashMap<>();
                                                     String mapNameTecnici = "tecnicoId2Technicians";
                                                     String mapModuleTecnici = "cbMap";
                                                     String conditionTecnici = "mapname" + "='" + mapNameTecnici + "'";
                                                     String queryMapTecnici = "select * from " + mapModuleTecnici + " where " + conditionTecnici;
                                                     JSONArray mapdataTecnici = wsClient.doQuery(queryMapTecnici);
                                                     JSONObject resultTecnici = (JSONObject)parser.parse(mapdataTecnici.get(0).toString());
                                                     JSONObject contentjsonTecnici = (JSONObject)parser.parse(resultTecnici.get("contentjson").toString());
                                                     JSONObject fieldsTecnici = (JSONObject)parser.parse(contentjsonTecnici.get("fields").toString());
                                                     JSONArray fields_arrayTecnici = (JSONArray) fieldsTecnici.get("field");
                                                     for (Object field: fields_arrayTecnici) {
                                                         JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                                         JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                                         if (((JSONObject) field).get("fieldname").toString().equals("techniciansrcid")) {
                                                             recordFieldTecnici.put(((JSONObject)field).get("fieldname").toString(),
                                                                     tecniciObject.get(originalFiled.get("OrgfieldName").toString()));
                                                         } else {
                                                             if (originalFiled.get("OrgfieldName").toString().equals("comune")) {
                                                                 if (indirizzoObject.get("comune") != null && !indirizzoObject.get("comune").toString().isEmpty()) {
                                                                     Map<String, Object> searchResultGeoboundary = searchRecord(
                                                                             "Geoboundary", indirizzoObject.get("comune").toString(),
                                                                             "geoname", "", false);
                                                                     Map<String, Object> searchResultGeoboundaryDefault = searchRecord("Geoboundary",
                                                                             "DA VERIFICARE", "geoname", "", false);
                                                                     if (((boolean) searchResultGeoboundary.get("status")) || ((boolean) searchResultGeoboundaryDefault.get("status"))) {
                                                                         Map<String, String> referenceFields = getUIType10Field("Technicians");
                                                                         for (Object key : referenceFields.keySet()) {
                                                                             String keyStr = (String)key;
                                                                             if (referenceFields.get(keyStr).equals("GeoBoundary")) {
                                                                                 if (((boolean) searchResultGeoboundary.get("status"))) {
                                                                                     recordFieldTecnici.put(keyStr, searchResultGeoboundary.get("crmid"));
                                                                                 } else {
                                                                                     recordFieldTecnici.put(keyStr, searchResultGeoboundaryDefault.get("crmid"));
                                                                                 }
                                                                             }
                                                                         }
                                                                     }
                                                                 } else {
                                                                     Map<String, Object> searchResultGeoboundaryDefault = searchRecord("Geoboundary",
                                                                             "DA VERIFICARE", "geoname", "", false);
                                                                     if (((boolean) searchResultGeoboundaryDefault.get("status"))) {
                                                                         Map<String, String> referenceFields = getUIType10Field("Technicians");
                                                                         for (Object key : referenceFields.keySet()) {
                                                                             String keyStr = (String)key;
                                                                             if (referenceFields.get(keyStr).equals("GeoBoundary")) {
                                                                                 recordFieldTecnici.put(keyStr, searchResultGeoboundaryDefault.get("crmid"));
                                                                             }
                                                                         }
                                                                     }
                                                                 }
                                                             } else {
                                                                 recordFieldTecnici.put(((JSONObject)field).get("fieldname").toString(),
                                                                         indirizzoObject.get(originalFiled.get("OrgfieldName").toString()));
                                                             }
                                                         }

                                                     }

                                                     recordFieldTecnici.put("assigned_user_id", wsClient.getUserID());
                                                     recordMapTecnici.put("elementType", "Technicians");
                                                     recordMapTecnici.put("element", Util.getJson(recordFieldTecnici));
                                                     recordMapTecnici.put("searchOn", "branchsrcid");
                                                     StringBuilder builderRemoveIndexZero = new StringBuilder(recordFieldTecnici.keySet().toString());
                                                     builderRemoveIndexZero.deleteCharAt(0);
                                                     StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                                     builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                                     String updatedfields = builderRemoveIndexLast.toString();
                                                     recordMapTecnici.put("updatedfields", updatedfields);
                                                     // System.out.println(recordMapTecnici);
                                                     Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapTecnici, "POST");
                                                     JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                     if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                                                         recordField.put("linktotechnician", obj.get("id").toString());
                                                     }
                                                 }
                                             }
                                         }
                                     }

                                 }

                             }

                             recordField.put("assigned_user_id", wsClient.getUserID());
                             recordField.put("created_user_id", wsClient.getUserID());
                             recordField.put("smownerid", wsClient.getUserID());
                             recordField.put("smcreatorid", wsClient.getUserID());
                             // System.out.println(recordField);
                             recordMap.put("elementType", moduleFieldInfo.get(fieldname));
                             recordMap.put("element", Util.getJson(recordField));
                             recordMap.put("searchOn", fieldToSearch.get(orgfieldName));
                             StringBuilder builderRemoveIndexZero = new StringBuilder(recordField.keySet().toString());
                             builderRemoveIndexZero.deleteCharAt(0);
                             StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                             builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                             String updatedfields = builderRemoveIndexLast.toString();
                             recordMap.put("updatedfields", updatedfields);
                             System.out.println("Record to Send:: " + recordMap);
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
                     }
                 } else {
                     // This Module contain reference field which depend on Main Module
                     uitype10fields = getUIType10Field(fieldname);
                     if (!uitype10fields.isEmpty()) {
                         Map<String, Object> objValue = (Map<String, Object>) element.get(orgfieldName);
                         Map<String, Object> recordToCreate =  getMapOfRecordToBeCreated(uitype10fields, fieldname,
                                 parentModule, objValue, fieldToSearch.get(orgfieldName).toString(), orgfieldName);
                         System.out.println("RECORD to CREATE::" + recordToCreate);
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
                // System.out.println(parser.parse(jsonValue));
                JSONArray recordsArray = (JSONArray) parser.parse(jsonValue);
                Map<String, Object> fieldToSearch = getSearchField(parentModule);
                // System.out.println(fieldToSearch);
                for (Object objRecord : recordsArray) {
                    if (objRecord instanceof JSONObject) {
                        uitype10fields = getUIType10Field(fieldname);
                        // System.out.println(uitype10fields);
                        // System.out.println(objRecord);
                        if (!uitype10fields.isEmpty()) {
                            Map<String, Object> objValue = (Map<String, Object>) objRecord;
                            // System.out.println(objValue);
                            // System.out.println("TESSSSS");
                            // System.out.println(uitype10fields);
                            // System.out.println(fieldname);
                            // System.out.println(parentModule);
                            // System.out.println(objValue);
                            // System.out.println(fieldToSearch.get(orgfieldName).toString());
                            // System.out.println(orgfieldName);
                            String fldsearch = "";
                            if (fieldToSearch.containsKey(orgfieldName)) {
                                fldsearch = fieldToSearch.get(orgfieldName).toString();
                            }
                            Map<String, Object> recordToCreate =  getMapOfRecordToBeCreated(uitype10fields, fieldname,
                                    parentModule, objValue, fldsearch, orgfieldName);
                            System.out.println("RECORD to CREATE::" + recordToCreate);
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
            } else {
                /*
                * filialePartenzaId Query cbCompany module in order to check whether there already exists a record where branchsrcid == filialePartenzaId.
                * If there exists none, then call the api/filiali endpoint and retrieve the object where ID == filialePartenzaId. The, create a new cbCompany based on the standard filiale mapping that we have defined in this task http://phabricator.studioevolutivo.it/T10384#192074.
                * Relate the Shipment (which has been just created) with that cbCompany by fillings its departurebranch with cbcompanyid of that cbCompany.
                * */
                if (orgfieldName.equals("filialePartenzaId")) {

                    if (Integer.parseInt(record.get(orgfieldName).toString()) == 0) {
                        rs.put("status", "notfound");
                        rs.put("value",  "");
                        return rs;
                    }
                    Map<String, Object> searchResultCompany = searchRecord("cbCompany",
                            record.get(orgfieldName).toString(), "branchsrcid", "", true);

                    System.out.println("Processing filialePartenzaId Response Key Data");
                    if (((boolean) searchResultCompany.get("status")) && !((boolean) searchResultCompany.get("mustbeupdated"))) {
                        Map<String, String> referenceFields = getUIType10Field(moduleFieldInfo.get(fieldname));
                        for (Object key : referenceFields.keySet()) {
                            String keyStr = (String)key;
                            if (referenceFields.get(keyStr).equals("cbCompany")) {
                                rs.put("status", "found");
                                rs.put("value", searchResultCompany.get("crmid"));
                            }
                        }
                    } else {
                        if (startRestService()) {
                            System.out.println("Processing filiali Response Data");
                            String endpoint = "filiali";
                            String objectKey = "filiali";
                            String id = record.get(orgfieldName).toString();

                            Object filialiResponse = doGet(restClient.get_servicetoken(), endpoint, objectKey);
                            // System.out.println(filialiResponse);
                            if (filialiResponse != null) {
                                // System.out.println("GETTING FILIALI");
                                Map<String, Object> filialiObject = searchByID(filialiResponse, id);
                                // System.out.println(filialiObject);
                                if (!filialiObject.isEmpty()) {
                                    Map<String, Object> recordMapFiliali = new HashMap<>();
                                    Map<String, Object> recordFieldFiliali = new HashMap<>();
                                    String mapNameFiliali = "filialeId2cbCompany";
                                    String mapModuleFiliali = "cbMap";
                                    String conditionFiliali = "mapname" + "='" + mapNameFiliali + "'";
                                    String queryMapFiliali = "select * from " + mapModuleFiliali + " where " + conditionFiliali;
                                    JSONArray mapdataFiliali = wsClient.doQuery(queryMapFiliali);
                                    JSONObject resultFiliali = (JSONObject)parser.parse(mapdataFiliali.get(0).toString());
                                    JSONObject contentjsonFiliali = (JSONObject)parser.parse(resultFiliali.get("contentjson").toString());
                                    JSONObject fieldsFiliali = (JSONObject)parser.parse(contentjsonFiliali.get("fields").toString());
                                    JSONArray fields_arrayFiliali = (JSONArray) fieldsFiliali.get("field");
                                    for (Object field: fields_arrayFiliali) {
                                        JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                        JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                        recordFieldFiliali.put(((JSONObject)field).get("fieldname").toString(), filialiObject.get(originalFiled.get("OrgfieldName").toString()));
                                    }

                                    // System.out.println(recordFieldFiliali);

                                    /*
                                     * Query GeoBoundary module and find the record where geoname == comune parameter of the API output.
                                     * Store in geobid field of the new cbCompany the value of geobid of the found GeoBoundary record
                                     * */
                                    if (((JSONObject) parser.parse(filialiObject.toString())).get("comune") != null &&
                                            !((JSONObject) parser.parse(filialiObject.toString())).get("comune").toString().isEmpty()) {
                                        Map<String, Object> searchResultGeoboundary = searchRecord("Geoboundary",
                                                ((JSONObject) parser.parse(filialiObject.toString())).get("comune").toString(),
                                                "geoname", "", false);

                                        // System.out.println(searchResultGeoboundary);
                                        /*
                                         * Otherwise, link the cbAddress with the GeoBoundary record where geoname == DA VERIFICARE
                                         * */
                                        Map<String, Object> searchResultGeoboundaryDefault = searchRecord(
                                                "Geoboundary", "DA VERIFICARE", "geoname",
                                                "", false);

                                        if (((boolean) searchResultGeoboundary.get("status")) || ((boolean) searchResultGeoboundaryDefault.get("status"))) {
                                            Map<String, String> referenceFields = getUIType10Field("cbCompany");
                                            for (Object key : referenceFields.keySet()) {
                                                String keyStr = (String)key;
                                                if (referenceFields.get(keyStr).equals("GeoBoundary")) {
                                                    if (((boolean) searchResultGeoboundary.get("status"))) {
                                                        recordFieldFiliali.put(keyStr, searchResultGeoboundary.get("crmid"));
                                                    } else {
                                                        recordFieldFiliali.put(keyStr, searchResultGeoboundaryDefault.get("crmid"));
                                                    }

                                                }
                                            }
                                        }
                                    } else {
                                        Map<String, Object> searchResultGeoboundaryDefault = searchRecord(
                                                "Geoboundary", "DA VERIFICARE", "geoname",
                                                "", false);
                                        if (((boolean) searchResultGeoboundaryDefault.get("status"))) {
                                            Map<String, String> referenceFields = getUIType10Field("cbCompany");
                                            for (Object key : referenceFields.keySet()) {
                                                String keyStr = (String)key;
                                                if (referenceFields.get(keyStr).equals("GeoBoundary")) {
                                                        recordFieldFiliali.put(keyStr, searchResultGeoboundaryDefault.get("crmid"));
                                                }
                                            }
                                        }
                                    }

                                    Map<String, Object> searchResultVendorModule;
                                    /*
                                     * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                                     * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                                     * vettoreId
                                     * */
                                    System.out.println(((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString());
                                    searchResultVendorModule = searchRecord("Vendors",
                                            ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString(),
                                            "suppliersrcid", "Vettore", true);

                                    // System.out.println(searchResultGeoboundary);
                                    if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                        recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                    } else {
                                        // To Search in Rest Service
                                        if (startRestService()) {
                                            System.out.println("Processing vettori Response Data");
                                            String vettoriEndpoint = "vettori";
                                            String vettoriDataKey = "vettori";

                                            Object vettoriResponse = doGet(restClient.get_servicetoken(), vettoriEndpoint, vettoriDataKey);
                                            // System.out.println(vettoriResponse);
                                            // System.out.println(filialiResponse);
                                            if (vettoriResponse != null) {
                                                Map<String, Object> vettoriObject = searchByID(vettoriResponse,
                                                        ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString());
                                                if (!vettoriObject.isEmpty()) {
                                                    Map<String, Object> vettoriRecordMap = new HashMap<>();
                                                    Map<String, Object> vettoriRecordField = new HashMap<>();
                                                    // String vettoriMapName = orgfieldName + "2" + fieldname;
                                                    String vettoriMapName = "vettoreId2Vendors";
                                                    String vettoriMapModule = "cbMap";
                                                    String vettoriCondition = "mapname" + "='" + vettoriMapName + "'";
                                                    String vettoriQueryMap = "select * from " + vettoriMapModule + " where " + vettoriCondition;
                                                    JSONArray vettoriMapData = wsClient.doQuery(vettoriQueryMap);
                                                    JSONObject vettoriQueryResult = (JSONObject)parser.parse(vettoriMapData.get(0).toString());
                                                    JSONObject vettoriMapContentJSON = (JSONObject)parser.parse(vettoriQueryResult.get("contentjson").toString());
                                                    JSONObject vettoriMapFields = (JSONObject)parser.parse(vettoriMapContentJSON.get("fields").toString());
                                                    JSONArray vettoriFieldsArray = (JSONArray) vettoriMapFields.get("field");
                                                    for (Object field: vettoriFieldsArray) {
                                                        JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                                        JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                                        vettoriRecordField.put(((JSONObject)field).get("fieldname").toString(), vettoriObject.get(originalFiled.get("OrgfieldName").toString()));
                                                    }

                                                    vettoriRecordField.put("assigned_user_id", wsClient.getUserID());
                                                    vettoriRecordField.put("type", "Vettore");
                                                    vettoriRecordMap.put("elementType", "Vendors");
                                                    vettoriRecordMap.put("element", Util.getJson(vettoriRecordField));
                                                    vettoriRecordMap.put("searchOn", "suppliersrcid");
                                                    StringBuilder builderRemoveIndexZero = new StringBuilder(vettoriRecordField.keySet().toString());
                                                    builderRemoveIndexZero.deleteCharAt(0);
                                                    StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                                    builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                                    String updatedfields = builderRemoveIndexLast.toString();
                                                    vettoriRecordMap.put("updatedfields", updatedfields);
                                                    Object newRecord = wsClient.doInvoke(Util.methodUPSERT, vettoriRecordMap, "POST");
                                                    JSONObject vettoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                    if (vettoriobjrec.containsKey("id") && !vettoriobjrec.get("id").toString().equals("")) {
                                                        recordFieldFiliali.put("linktocarrier", vettoriobjrec.get("id").toString());
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    /*
                                     * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                                     * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                                     * fornitoreId
                                     * */
                                    searchResultVendorModule = searchRecord("Vendors",
                                            ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString(),
                                            "suppliersrcid", "Fornitore", true);
                                    if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                        recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                    } else {
                                        // To Search in Rest Service
                                        if (startRestService()) {
                                            System.out.println("Processing fornitori Response Data");
                                            String fornitoriEndpoint = "fornitori";
                                            String fornitoriDataKey = "fornitori";

                                            Object fornitoriResponse = doGet(restClient.get_servicetoken(), fornitoriEndpoint, fornitoriDataKey);
                                            if (fornitoriResponse != null) {
                                                Map<String, Object> fornitoriObject = searchByID(fornitoriResponse,
                                                        ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString());
                                                if (!fornitoriObject.isEmpty()) {
                                                    Map<String, Object> fornitoriRecordMap = new HashMap<>();
                                                    Map<String, Object> fornitoriRecordField = new HashMap<>();
                                                    //String fornitoriMapName = orgfieldName + "2" + fieldname;
                                                    String fornitoriMapName = "fornitoreId2Vendors";
                                                    String fornitoriMapModule = "cbMap";
                                                    String fornitoriCondition = "mapname" + "='" + fornitoriMapName + "'";
                                                    String fornitoriQueryMap = "select * from " + fornitoriMapModule + " where " + fornitoriCondition;
                                                    JSONArray fornitoriMapData = wsClient.doQuery(fornitoriQueryMap);
                                                    JSONObject fornitoriQueryResult = (JSONObject)parser.parse(fornitoriMapData.get(0).toString());
                                                    JSONObject fornitoriMapContentJSON = (JSONObject)parser.parse(fornitoriQueryResult.get("contentjson").toString());
                                                    JSONObject fornitoriMapFields = (JSONObject)parser.parse(fornitoriMapContentJSON.get("fields").toString());
                                                    JSONArray fornitoriFieldsArray = (JSONArray) fornitoriMapFields.get("field");
                                                    for (Object field: fornitoriFieldsArray) {
                                                        JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                                        JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                                        fornitoriRecordField.put(((JSONObject)field).get("fieldname").toString(), fornitoriObject.get(originalFiled.get("OrgfieldName").toString()));
                                                    }

                                                    fornitoriRecordField.put("assigned_user_id", wsClient.getUserID());
                                                    fornitoriRecordField.put("type", "Vettore");
                                                    fornitoriRecordMap.put("elementType", "Vendors");
                                                    fornitoriRecordMap.put("element", Util.getJson(fornitoriRecordField));
                                                    fornitoriRecordMap.put("searchOn", "suppliersrcid");
                                                    StringBuilder builderRemoveIndexZero = new StringBuilder(fornitoriRecordField.keySet().toString());
                                                    builderRemoveIndexZero.deleteCharAt(0);
                                                    StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                                    builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                                    String updatedfields = builderRemoveIndexLast.toString();
                                                    fornitoriRecordMap.put("updatedfields", updatedfields);
                                                    Object newRecord = wsClient.doInvoke(Util.methodUPSERT, fornitoriRecordMap, "POST");
                                                    JSONObject fornitoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                    if (fornitoriobjrec.containsKey("id") && !fornitoriobjrec.get("id").toString().equals("")) {
                                                        recordFieldFiliali.put("vendorid", fornitoriobjrec.get("id").toString());
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    recordFieldFiliali.put("assigned_user_id", wsClient.getUserID());
                                    recordMapFiliali.put("elementType", "cbCompany");
                                    recordMapFiliali.put("element", Util.getJson(recordFieldFiliali));
                                    recordMapFiliali.put("searchOn", "branchsrcid");
                                    StringBuilder builderRemoveIndexZero = new StringBuilder(recordFieldFiliali.keySet().toString());
                                    builderRemoveIndexZero.deleteCharAt(0);
                                    StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                    builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                    String updatedfields = builderRemoveIndexLast.toString();
                                    recordMapFiliali.put("updatedfields", updatedfields);
                                    // System.out.println(recordMapFiliali);
                                    Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapFiliali, "POST");
                                    JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                                    if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                                        rs.put("status", "found");
                                        rs.put("value", obj.get("id").toString());
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // System.out.println(orgfieldName);
                    rs.put("status", "found");
                    if (moduleDateFields.containsKey(fieldname) && (moduleDateFields.get(fieldname).equals("5") || moduleDateFields.get(fieldname).equals("50"))) {
                        String dateValue = record.get(orgfieldName).toString().replace("T", " ");
                        rs.put("value",  dateValue);
                    } else {
                        rs.put("value",  record.get(orgfieldName));
                    }
                }
            }
            return rs;
        } else {
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

    private Map<String, Object> searchRecord(String module, String value, String fieldname, String otherCondition,
                                             boolean mustBeUpdated) throws ParseException {
        Map<String, Object> result = new HashMap<>();
        // Check if value contain any Special character especially '
        // if (value == null) {
            // result.put("status", false);
            // result.put("crmid", "");
            // return result;
        // }


        if (value.contains("'")) {
            int specialCharPosition = value.indexOf("'") + 1;
            StringBuffer stringBuffer= new StringBuffer(value);
            value = stringBuffer.insert(specialCharPosition, "'").toString();
        }
        String condition;
        if (module.equals("Vendors")) {
            if  (otherCondition.isEmpty()) {
                condition = fieldname + "='" + value + "'"  + "AND type ='Fornitore'";
            } else {
                condition = fieldname + "='" + value + "'"  + "AND type ='" + otherCondition + "'";
            }

        } else if (module.equals("cbEmployee")) {
            condition = fieldname + "='" + value + "'"  + "AND emptype ='" + otherCondition + "'";
        } else if (module.equals("ProcessLog")) {
            condition = otherCondition;
        } else {
            condition = fieldname + "='" + value + "'";
        }
        String queryMap = "select * from " + module + " where " + condition;
        // System.out.println(queryMap);
        JSONArray mapdata = wsClient.doQuery(queryMap);
        // System.out.println(mapdata);
        if (mapdata.size() == 0) {
            result.put("status", false);
            result.put("crmid", "");
        } else {
            JSONParser parser = new JSONParser();
            JSONObject queryResult = (JSONObject)parser.parse(mapdata.get(0).toString());
            // System.out.println(queryResult);
            // System.out.println(queryResult.get("id").toString());
            String crmid = queryResult.get("id").toString();
            // System.out.println(crmid);
            if (!crmid.isEmpty()) {
                result.put("status", true);
            } else {
                result.put("status", false);
            }
            result.put("crmid", crmid);
            result.put("mustbeupdated", mustBeUpdated);
            // result.put("mustbeupdated", false);
        }
        // System.out.println(result);
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
        // System.out.println("Get Module Date Fields");
        // System.out.println(this.moduleDateFields);
    }

    private Map<String, Object> getMapOfRecordToBeCreated(Map<String, String> moduleFieldInfo, String fieldname,
                                                          String parentModule, Map element, String fieldToSearch,
                                                          String orgfieldName) throws Exception {
        // We Have to Create the New Record and get Its CRMID
        // String modulesIdField = Objects.requireNonNull(modulesDeclared).getFieldsDoQuery(moduleFieldInfo.get(fieldname)).get(0);
        // System.out.println(moduleFieldInfo);
        // System.out.println(fieldname);
        // System.out.println(parentModule);
        // System.out.println(element);
        // System.out.println(fieldToSearch);
        // System.out.println(orgfieldName);

        Map<String, Object> recordMap = new HashMap<>();
        Map<String, Object> recordField = new HashMap<>();
        JSONParser parser = new JSONParser();
        // Get Map for Adding that Module from Rest API
        String mapName = orgfieldName + "2" + fieldname;
        // System.out.println(mapName);
        String mapModule = "cbMap";
        String condition = "mapname" + "='" + mapName + "'";
        String queryMap = "select * from " + mapModule + " where " + condition;
        // System.out.println(queryMap);
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

        // System.out.println(recordField);
        // System.out.println(element);

        if (orgfieldName.equals("prenotazioni")) {
            /*
             * Query cbCompany module in order to check whether there already exists a record where branchsrcid == filialeId.
             */
            System.out.println("Processing prenotazioni Response key Data");
            // System.out.println("PRENOTAZIONI");
            // System.out.println(element);
            JSONObject prenotazioni = (JSONObject) parser.parse(element.toString());
            // System.out.println(prenotazioni);
            if (prenotazioni.get("restFiliale") instanceof JSONObject) {
                JSONObject restFiliale = (JSONObject) prenotazioni.get("restFiliale");
                // System.out.println(restFiliale);
                Map<String, Object> searchResultCompany = searchRecord("cbCompany",
                        restFiliale.get("ID").toString(), "branchsrcid", "", true);

                // System.out.println("PRENOTAZIONI");

                if (((boolean) searchResultCompany.get("status")) && !((boolean) searchResultCompany.get("mustbeupdated"))) {
                    Map<String, String> referenceFields = getUIType10Field(fieldname);
                    for (Object key : referenceFields.keySet()) {
                        String keyStr = (String)key;
                        if (referenceFields.get(keyStr).equals("cbCompany")) {
                            recordField.put(keyStr, searchResultCompany.get("crmid"));
                        }
                    }
                } else {
                    /*
                     * Query cbCompany module in order to check whether there already exists a record where branchsrcid == restFiliale.ID.
                     * If there exists none, then create a new one following the indications described at filialeId section above.
                     */
                    if (prenotazioni.get("restFiliale") instanceof JSONObject) {
                        Map<String, Object> filialiObject = (Map<String, Object>) prenotazioni.get("restFiliale");
                        // System.out.println(filialiObject);
                        if (!filialiObject.isEmpty()) {
                            Map<String, Object> recordMapFiliali = new HashMap<>();
                            Map<String, Object> recordFieldFiliali = new HashMap<>();
                            String mapNameFiliali = "filialeId2cbCompany";
                            String mapModuleFiliali = "cbMap";
                            String conditionFiliali = "mapname" + "='" + mapNameFiliali + "'";
                            String queryMapFiliali = "select * from " + mapModuleFiliali + " where " + conditionFiliali;
                            JSONArray mapdataFiliali = wsClient.doQuery(queryMapFiliali);
                            JSONObject resultFiliali = (JSONObject)parser.parse(mapdataFiliali.get(0).toString());
                            JSONObject contentjsonFiliali = (JSONObject)parser.parse(resultFiliali.get("contentjson").toString());
                            JSONObject fieldsFiliali = (JSONObject)parser.parse(contentjsonFiliali.get("fields").toString());
                            JSONArray fields_arrayFiliali = (JSONArray) fieldsFiliali.get("field");
                            for (Object field: fields_arrayFiliali) {
                                JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                recordFieldFiliali.put(((JSONObject)field).get("fieldname").toString(), filialiObject.get(originalFiled.get("OrgfieldName").toString()));
                            }

                            /*
                             * Query GeoBoundary module and find the record where geoname == comune parameter of the API output.
                             * Store in geobid field of the new cbCompany the value of geobid of the found GeoBoundary record
                             * */
                            Map<String, Object> searchResultGeoboundary = searchRecord("Geoboundary",
                                    ((JSONObject) parser.parse(filialiObject.toString())).get("comune").toString(),
                                    "geoname", "", false);
                            Map<String, Object> searchResultGeoboundaryDefault = searchRecord("Geoboundary",
                                    "DA VERIFICARE", "geoname", "", false);
                            if (((boolean) searchResultGeoboundary.get("status")) || ((boolean) searchResultGeoboundaryDefault.get("status"))) {
                                Map<String, String> referenceFields = getUIType10Field("cbCompany");
                                for (Object key : referenceFields.keySet()) {
                                    String keyStr = (String)key;
                                    if (referenceFields.get(keyStr).equals("GeoBoundary")) {
                                        if (((boolean) searchResultGeoboundary.get("status"))) {
                                            recordFieldFiliali.put(keyStr, searchResultGeoboundary.get("crmid"));
                                        } else {
                                            recordFieldFiliali.put(keyStr, searchResultGeoboundaryDefault.get("crmid"));
                                        }

                                    }
                                }
                            }

                            Map<String, Object> searchResultVendorModule;
                            /*
                             * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                             * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                             * vettoreId
                             * */
                            // System.out.println(((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString());
                            searchResultVendorModule = searchRecord("Vendors",
                                    ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString(),
                                    "suppliersrcid", "Vettore", true);

                            // System.out.println(searchResultGeoboundary);
                            if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                            } else {
                                // To Search in Rest Service
                                if (startRestService()) {
                                    String vettoriEndpoint = "vettori";
                                    String vettoriDataKey = "vettori";

                                    Object vettoriResponse = doGet(restClient.get_servicetoken(), vettoriEndpoint, vettoriDataKey);
                                    // System.out.println(vettoriResponse);
                                    if (vettoriResponse != null) {
                                        Map<String, Object> vettoriObject = searchByID(vettoriResponse,
                                                ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString());
                                        if (!vettoriObject.isEmpty()) {
                                            Map<String, Object> vettoriRecordMap = new HashMap<>();
                                            Map<String, Object> vettoriRecordField = new HashMap<>();
                                            // String vettoriMapName = orgfieldName + "2" + fieldname;
                                            String vettoriMapName = "vettoreId2Vendors";
                                            String vettoriMapModule = "cbMap";
                                            String vettoriCondition = "mapname" + "='" + vettoriMapName + "'";
                                            String vettoriQueryMap = "select * from " + vettoriMapModule + " where " + vettoriCondition;
                                            JSONArray vettoriMapData = wsClient.doQuery(vettoriQueryMap);
                                            JSONObject vettoriQueryResult = (JSONObject)parser.parse(vettoriMapData.get(0).toString());
                                            JSONObject vettoriMapContentJSON = (JSONObject)parser.parse(vettoriQueryResult.get("contentjson").toString());
                                            JSONObject vettoriMapFields = (JSONObject)parser.parse(vettoriMapContentJSON.get("fields").toString());
                                            JSONArray vettoriFieldsArray = (JSONArray) vettoriMapFields.get("field");
                                            for (Object field: vettoriFieldsArray) {
                                                JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                                JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                                vettoriRecordField.put(((JSONObject)field).get("fieldname").toString(), vettoriObject.get(originalFiled.get("OrgfieldName").toString()));
                                            }

                                            vettoriRecordField.put("assigned_user_id", wsClient.getUserID());
                                            vettoriRecordField.put("type", "Vettore");
                                            vettoriRecordMap.put("elementType", "Vendors");
                                            vettoriRecordMap.put("element", Util.getJson(vettoriRecordField));
                                            vettoriRecordMap.put("searchOn", "suppliersrcid");
                                            StringBuilder builderRemoveIndexZero = new StringBuilder(vettoriRecordField.keySet().toString());
                                            builderRemoveIndexZero.deleteCharAt(0);
                                            StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                            builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                            String updatedfields = builderRemoveIndexLast.toString();
                                            vettoriRecordMap.put("updatedfields", updatedfields);
                                            Object newRecord = wsClient.doInvoke(Util.methodUPSERT, vettoriRecordMap, "POST");
                                            JSONObject vettoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                            if (vettoriobjrec.containsKey("id") && !vettoriobjrec.get("id").toString().equals("")) {
                                                recordFieldFiliali.put("linktocarrier", vettoriobjrec.get("id").toString());
                                            }
                                        }
                                    }
                                }
                            }

                            /*
                             * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                             * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                             * fornitoreId
                             * */
                            searchResultVendorModule = searchRecord("Vendors",
                                    ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString(),
                                    "suppliersrcid", "Fornitore", true);
                            if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                            } else {
                                // To Search in Rest Service
                                if (startRestService()) {
                                    String fornitoriEndpoint = "fornitori";
                                    String fornitoriDataKey = "fornitori";

                                    Object fornitoriResponse = doGet(restClient.get_servicetoken(), fornitoriEndpoint, fornitoriDataKey);
                                    if (fornitoriResponse != null) {
                                        Map<String, Object> fornitoriObject = searchByID(fornitoriResponse,
                                                ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString());
                                        if (!fornitoriObject.isEmpty()) {
                                            Map<String, Object> fornitoriRecordMap = new HashMap<>();
                                            Map<String, Object> fornitoriRecordField = new HashMap<>();
                                            // String fornitoriMapName = orgfieldName + "2" + fieldname;
                                            String fornitoriMapName = "fornitoreId2Vendors";
                                            String fornitoriMapModule = "cbMap";
                                            String fornitoriCondition = "mapname" + "='" + fornitoriMapName + "'";
                                            String fornitoriQueryMap = "select * from " + fornitoriMapModule + " where " + fornitoriCondition;
                                            JSONArray fornitoriMapData = wsClient.doQuery(fornitoriQueryMap);
                                            JSONObject fornitoriQueryResult = (JSONObject)parser.parse(fornitoriMapData.get(0).toString());
                                            JSONObject fornitoriMapContentJSON = (JSONObject)parser.parse(fornitoriQueryResult.get("contentjson").toString());
                                            JSONObject fornitoriMapFields = (JSONObject)parser.parse(fornitoriMapContentJSON.get("fields").toString());
                                            JSONArray fornitoriFieldsArray = (JSONArray) fornitoriMapFields.get("field");
                                            for (Object field: fornitoriFieldsArray) {
                                                JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                                JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                                fornitoriRecordField.put(((JSONObject)field).get("fieldname").toString(), fornitoriObject.get(originalFiled.get("OrgfieldName").toString()));
                                            }

                                            fornitoriRecordField.put("assigned_user_id", wsClient.getUserID());
                                            fornitoriRecordField.put("type", "Vettore");
                                            fornitoriRecordMap.put("elementType", "Vendors");
                                            fornitoriRecordMap.put("element", Util.getJson(fornitoriRecordField));
                                            fornitoriRecordMap.put("searchOn", "suppliersrcid");
                                            StringBuilder builderRemoveIndexZero = new StringBuilder(fornitoriRecordField.keySet().toString());
                                            builderRemoveIndexZero.deleteCharAt(0);
                                            StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                            builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                            String updatedfields = builderRemoveIndexLast.toString();
                                            fornitoriRecordMap.put("updatedfields", updatedfields);
                                            Object newRecord = wsClient.doInvoke(Util.methodUPSERT, fornitoriRecordMap, "POST");
                                            JSONObject fornitoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                            if (fornitoriobjrec.containsKey("id") && !fornitoriobjrec.get("id").toString().equals("")) {
                                                recordFieldFiliali.put("vendorid", fornitoriobjrec.get("id").toString());
                                            }
                                        }
                                    }
                                }
                            }

                            recordFieldFiliali.put("assigned_user_id", wsClient.getUserID());
                            recordMapFiliali.put("elementType", "cbCompany");
                            recordMapFiliali.put("element", Util.getJson(recordFieldFiliali));
                            recordMapFiliali.put("searchOn", "branchsrcid");
                            StringBuilder builderRemoveIndexZero = new StringBuilder(recordFieldFiliali.keySet().toString());
                            builderRemoveIndexZero.deleteCharAt(0);
                            StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                            builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                            String updatedfields = builderRemoveIndexLast.toString();
                            recordMapFiliali.put("updatedfields", updatedfields);
                            // System.out.println(recordMapFiliali);
                            Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapFiliali, "POST");
                            JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                            if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                                recordField.put("linktobranch", obj.get("id").toString());
                            }
                        }
                    }
                }
            }


            if (prenotazioni.get("restAutista") instanceof JSONObject) {
                System.out.println("Processing restAutista Response Data");
                /*
                 * Query cbEmployee module in order to check whether there already exists a record where nif == restAutista.ID AND emptype == 'Autista'.
                 * If there exists none, then create a new one with the following mapping:
                 * */
                JSONObject restAutista = (JSONObject) prenotazioni.get("restAutista");
                // System.out.println(restAutista);
                Map<String, Object> searchResultEmployee = searchRecord("cbEmployee",
                        restAutista.get("ID").toString(), "nif", "Autista", true);

                if (((boolean) searchResultEmployee.get("status")) && !((boolean) searchResultEmployee.get("mustbeupdated"))) {
                    Map<String, String> referenceFields = getUIType10Field(fieldname);
                    for (Object key : referenceFields.keySet()) {
                        String keyStr = (String)key;
                        if (referenceFields.get(keyStr).equals("cbEmployee")) {
                            recordField.put(keyStr, searchResultEmployee.get("crmid"));
                        }
                    }
                } else {
                    /*
                     * Query cbCompany module in order to check whether there already exists a record where branchsrcid == restFiliale.ID.
                     * If there exists none, then create a new one following the indications described at filialeId section above.
                     */
                    Map<String, Object> recordMapRestAutista = new HashMap<>();
                    Map<String, Object> recordFieldRestAutista = new HashMap<>();
                    String mapNameRestAutista = "restAutista2cbEmployee";
                    String mapModuleRestAutista = "cbMap";
                    String conditionRestAutista = "mapname" + "='" + mapNameRestAutista + "'";
                    String queryMapRestAutista = "select * from " + mapModuleRestAutista + " where " + conditionRestAutista;
                    JSONArray mapdataRestAutista = wsClient.doQuery(queryMapRestAutista);
                    JSONObject resultRestAutista = (JSONObject)parser.parse(mapdataRestAutista.get(0).toString());
                    JSONObject contentjsonRestAutista = (JSONObject)parser.parse(resultRestAutista.get("contentjson").toString());
                    JSONObject fieldsRestAutista = (JSONObject)parser.parse(contentjsonRestAutista.get("fields").toString());
                    JSONArray fields_arrayRestAutista = (JSONArray) fieldsRestAutista.get("field");
                    for (Object field: fields_arrayRestAutista) {
                        JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                        JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                        recordFieldRestAutista.put(((JSONObject)field).get("fieldname").toString(), restAutista.get(originalFiled.get("OrgfieldName").toString()));
                    }


                    /*
                     * As regards filialeId, process it in the same way as desribed in the filialeId section above, and fill
                     * the linktobranch field of cbEmployee with cbcompanyid of the cbCompany module.
                     * */
                    if (startRestService()) {
                        if (parser.parse(restAutista.toString()) != null) {
                            System.out.println("Processing filiali Response Data");
                            String endpoint = "filiali";
                            String objectKey = "filiali";
                            String id = restAutista.get("filialeId").toString();
                            Object filialiResponse = doGet(restClient.get_servicetoken(), endpoint, objectKey);
                            if (filialiResponse != null) {
                                Map<String, Object> filialiObject = searchByID(filialiResponse, id);
                                if (!filialiObject.isEmpty()) {
                                    Map<String, Object> recordMapFiliali = new HashMap<>();
                                    Map<String, Object> recordFieldFiliali = new HashMap<>();
                                    String mapNameFiliali = "filialeId2cbCompany";
                                    String mapModuleFiliali = "cbMap";
                                    String conditionFiliali = "mapname" + "='" + mapNameFiliali + "'";
                                    String queryMapFiliali = "select * from " + mapModuleFiliali + " where " + conditionFiliali;
                                    JSONArray mapdataFiliali = wsClient.doQuery(queryMapFiliali);
                                    JSONObject resultFiliali = (JSONObject)parser.parse(mapdataFiliali.get(0).toString());
                                    JSONObject contentjsonFiliali = (JSONObject)parser.parse(resultFiliali.get("contentjson").toString());
                                    JSONObject fieldsFiliali = (JSONObject)parser.parse(contentjsonFiliali.get("fields").toString());
                                    JSONArray fields_arrayFiliali = (JSONArray) fieldsFiliali.get("field");
                                    for (Object field: fields_arrayFiliali) {
                                        JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                        JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                        recordFieldFiliali.put(((JSONObject)field).get("fieldname").toString(), filialiObject.get(originalFiled.get("OrgfieldName").toString()));
                                    }

                                    /*
                                     * Query GeoBoundary module and find the record where geoname == comune parameter of the API output.
                                     * Store in geobid field of the new cbCompany the value of geobid of the found GeoBoundary record
                                     * */
                                    Map<String, Object> searchResultGeoboundary = searchRecord("Geoboundary",
                                            ((JSONObject) parser.parse(filialiObject.toString())).get("comune").toString(),
                                            "geoname", "", false);
                                    Map<String, Object> searchResultGeoboundaryDefault = searchRecord("Geoboundary",
                                            "DA VERIFICARE", "geoname", "", false);
                                    if (((boolean) searchResultGeoboundary.get("status")) || ((boolean) searchResultGeoboundaryDefault.get("status"))) {
                                        Map<String, String> referenceFields = getUIType10Field("cbCompany");
                                        for (Object key : referenceFields.keySet()) {
                                            String keyStr = (String)key;
                                            if (referenceFields.get(keyStr).equals("GeoBoundary")) {
                                                if (((boolean) searchResultGeoboundary.get("status"))) {
                                                    recordFieldFiliali.put(keyStr, searchResultGeoboundary.get("crmid"));
                                                } else {
                                                    recordFieldFiliali.put(keyStr, searchResultGeoboundaryDefault.get("crmid"));
                                                }
                                            }
                                        }
                                    }

                                    Map<String, Object> searchResultVendorModule;
                                    /*
                                     * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                                     * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                                     * vettoreId
                                     * */
                                    // System.out.println(((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString());
                                    searchResultVendorModule = searchRecord("Vendors",
                                            ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString(),
                                            "suppliersrcid", "Vettore", true);

                                    // System.out.println(searchResultGeoboundary);
                                    if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                        recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                    } else {
                                        // To Search in Rest Service
                                        if (startRestService()) {
                                            String vettoriEndpoint = "vettori";
                                            String vettoriDataKey = "vettori";

                                            Object vettoriResponse = doGet(restClient.get_servicetoken(), vettoriEndpoint, vettoriDataKey);
                                            // System.out.println(vettoriResponse);
                                            if (vettoriResponse != null) {
                                                Map<String, Object> vettoriObject = searchByID(vettoriResponse,
                                                        ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString());
                                                if (!vettoriObject.isEmpty()) {
                                                    Map<String, Object> vettoriRecordMap = new HashMap<>();
                                                    Map<String, Object> vettoriRecordField = new HashMap<>();
                                                    // String vettoriMapName = orgfieldName + "2" + fieldname;
                                                    String vettoriMapName = "vettoreId2Vendors";
                                                    String vettoriMapModule = "cbMap";
                                                    String vettoriCondition = "mapname" + "='" + vettoriMapName + "'";
                                                    String vettoriQueryMap = "select * from " + vettoriMapModule + " where " + vettoriCondition;
                                                    JSONArray vettoriMapData = wsClient.doQuery(vettoriQueryMap);
                                                    JSONObject vettoriQueryResult = (JSONObject)parser.parse(vettoriMapData.get(0).toString());
                                                    JSONObject vettoriMapContentJSON = (JSONObject)parser.parse(vettoriQueryResult.get("contentjson").toString());
                                                    JSONObject vettoriMapFields = (JSONObject)parser.parse(vettoriMapContentJSON.get("fields").toString());
                                                    JSONArray vettoriFieldsArray = (JSONArray) vettoriMapFields.get("field");
                                                    for (Object field: vettoriFieldsArray) {
                                                        JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                                        JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                                        vettoriRecordField.put(((JSONObject)field).get("fieldname").toString(), vettoriObject.get(originalFiled.get("OrgfieldName").toString()));
                                                    }

                                                    vettoriRecordField.put("assigned_user_id", wsClient.getUserID());
                                                    vettoriRecordField.put("type", "Vettore");
                                                    vettoriRecordMap.put("elementType", "Vendors");
                                                    vettoriRecordMap.put("element", Util.getJson(vettoriRecordField));
                                                    vettoriRecordMap.put("searchOn", "suppliersrcid");
                                                    StringBuilder builderRemoveIndexZero = new StringBuilder(vettoriRecordField.keySet().toString());
                                                    builderRemoveIndexZero.deleteCharAt(0);
                                                    StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                                    builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                                    String updatedfields = builderRemoveIndexLast.toString();
                                                    vettoriRecordMap.put("updatedfields", updatedfields);
                                                    Object newRecord = wsClient.doInvoke(Util.methodUPSERT, vettoriRecordMap, "POST");
                                                    JSONObject vettoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                    if (vettoriobjrec.containsKey("id") && !vettoriobjrec.get("id").toString().equals("")) {
                                                        recordFieldFiliali.put("linktocarrier", vettoriobjrec.get("id").toString());
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    /*
                                     * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                                     * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                                     * fornitoreId
                                     * */
                                    searchResultVendorModule = searchRecord("Vendors",
                                            ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString(),
                                            "suppliersrcid", "Fornitore", true);
                                    if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                        recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                    } else {
                                        // To Search in Rest Service
                                        if (startRestService()) {
                                            String fornitoriEndpoint = "fornitori";
                                            String fornitoriDataKey = "fornitori";

                                            Object fornitoriResponse = doGet(restClient.get_servicetoken(), fornitoriEndpoint, fornitoriDataKey);
                                            if (fornitoriResponse != null) {
                                                Map<String, Object> fornitoriObject = searchByID(fornitoriResponse,
                                                        ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString());
                                                if (!fornitoriObject.isEmpty()) {
                                                    Map<String, Object> fornitoriRecordMap = new HashMap<>();
                                                    Map<String, Object> fornitoriRecordField = new HashMap<>();
                                                    // String fornitoriMapName = orgfieldName + "2" + fieldname;
                                                    String fornitoriMapName = "fornitoreId2Vendors";
                                                    String fornitoriMapModule = "cbMap";
                                                    String fornitoriCondition = "mapname" + "='" + fornitoriMapName + "'";
                                                    String fornitoriQueryMap = "select * from " + fornitoriMapModule + " where " + fornitoriCondition;
                                                    JSONArray fornitoriMapData = wsClient.doQuery(fornitoriQueryMap);
                                                    JSONObject fornitoriQueryResult = (JSONObject)parser.parse(fornitoriMapData.get(0).toString());
                                                    JSONObject fornitoriMapContentJSON = (JSONObject)parser.parse(fornitoriQueryResult.get("contentjson").toString());
                                                    JSONObject fornitoriMapFields = (JSONObject)parser.parse(fornitoriMapContentJSON.get("fields").toString());
                                                    JSONArray fornitoriFieldsArray = (JSONArray) fornitoriMapFields.get("field");
                                                    for (Object field: fornitoriFieldsArray) {
                                                        JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                                        JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                                        fornitoriRecordField.put(((JSONObject)field).get("fieldname").toString(), fornitoriObject.get(originalFiled.get("OrgfieldName").toString()));
                                                    }

                                                    fornitoriRecordField.put("assigned_user_id", wsClient.getUserID());
                                                    fornitoriRecordField.put("type", "Vettore");
                                                    fornitoriRecordMap.put("elementType", "Vendors");
                                                    fornitoriRecordMap.put("element", Util.getJson(fornitoriRecordField));
                                                    fornitoriRecordMap.put("searchOn", "suppliersrcid");
                                                    StringBuilder builderRemoveIndexZero = new StringBuilder(fornitoriRecordField.keySet().toString());
                                                    builderRemoveIndexZero.deleteCharAt(0);
                                                    StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                                    builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                                    String updatedfields = builderRemoveIndexLast.toString();
                                                    fornitoriRecordMap.put("updatedfields", updatedfields);
                                                    Object newRecord = wsClient.doInvoke(Util.methodUPSERT, fornitoriRecordMap, "POST");
                                                    JSONObject fornitoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                    if (fornitoriobjrec.containsKey("id") && !fornitoriobjrec.get("id").toString().equals("")) {
                                                        recordFieldFiliali.put("vendorid", fornitoriobjrec.get("id").toString());
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    recordFieldFiliali.put("assigned_user_id", wsClient.getUserID());
                                    recordMapFiliali.put("elementType", "cbCompany");
                                    recordMapFiliali.put("element", Util.getJson(recordFieldFiliali));
                                    recordMapFiliali.put("searchOn", "branchsrcid");
                                    StringBuilder builderRemoveIndexZero = new StringBuilder(recordFieldFiliali.keySet().toString());
                                    builderRemoveIndexZero.deleteCharAt(0);
                                    StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                    builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                    String updatedfields = builderRemoveIndexLast.toString();
                                    recordMapFiliali.put("updatedfields", updatedfields);
                                    // System.out.println(recordMapFiliali);
                                    Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapFiliali, "POST");
                                    JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                                    if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                                        recordMapRestAutista.put("linktobranch", obj.get("id").toString());
                                    }
                                }
                            }
                        }
                    }

                    recordFieldRestAutista.put("assigned_user_id", wsClient.getUserID());
                    recordMapRestAutista.put("elementType", "cbEmployee");
                    recordMapRestAutista.put("element", Util.getJson(recordFieldRestAutista));
                    recordMapRestAutista.put("searchOn", "nif");
                    StringBuilder builderRemoveIndexZero = new StringBuilder(recordFieldRestAutista.keySet().toString());
                    builderRemoveIndexZero.deleteCharAt(0);
                    StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                    builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                    String updatedfields = builderRemoveIndexLast.toString();
                    recordMapRestAutista.put("updatedfields", updatedfields);
                    Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapRestAutista, "POST");
                    JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                    if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                        recordField.put("linktodriver", obj.get("id").toString());
                    }
                }
            }
        }

        /*
        * http://phabricator.studioevolutivo.it/T10781
        * Query cbproductcategory module in order to find the record where categorysrcid == categoryId.
        * If there exists none, then make an HTTP request to GET /rest/categorieMerceologiche and retrieve the object where ID == categoryId.
        * Afterwards, create a new cbproductcategory record in CoreBOS with the following mapping:
        * */
        if (orgfieldName.equals("prodotti")) {
            System.out.println("Processing prodotti Response Key Data");
            JSONObject prodottiObject = (JSONObject) parser.parse(element.toString());
            if (prodottiObject.get("categoryId") != null) {
                Map<String, Object> searchResultCbproductcategory = searchRecord("cbproductcategory",
                        prodottiObject.get("categoryId").toString(), "categorysrcid", "", true);

                if (((boolean) searchResultCbproductcategory.get("status")) && !((boolean) searchResultCbproductcategory.get("mustbeupdated"))) {
                    Map<String, String> referenceFields = getUIType10Field(fieldname);
                    for (Object key : referenceFields.keySet()) {
                        String keyStr = (String)key;
                        if (referenceFields.get(keyStr).equals("cbproductcategory")) {
                            recordField.put(keyStr, searchResultCbproductcategory.get("crmid"));
                        }
                    }
                } else {
                    if (startRestService() && prodottiObject.get("categoryId") != null) {
                        System.out.println("Processing categoryId Response Data");
                        String endpoint = "categorieMerceologiche";
                        String objectKey = "categorieMerceologiche";
                        String id = prodottiObject.get("categoryId").toString();

                        // System.out.println("CATEGORY PRDUCT :: "+ id);
                        Object categorieMerceologicheResponse = doGet(restClient.get_servicetoken(), endpoint, objectKey);
                        // System.out.println(categorieMerceologicheResponse);
                        if (categorieMerceologicheResponse != null) {
                            // System.out.println("GETTING FILIALI");
                            Map<String, Object> categorieMerceologicheObject = searchByID(categorieMerceologicheResponse, id);
                            // System.out.println(categorieMerceologicheObject);
                            if (!categorieMerceologicheObject.isEmpty()) {
                                Map<String, Object> recordMapCategoryId = new HashMap<>();
                                Map<String, Object> recordFieldCategoryId = new HashMap<>();
                                String mapNameCategoryId = "categoryId2cbproductcategory";
                                String mapModuleCategoryId = "cbMap";
                                String conditionCategoryId = "mapname" + "='" + mapNameCategoryId + "'";
                                String queryMapCategoryId = "select * from " + mapModuleCategoryId + " where " + conditionCategoryId;
                                JSONArray mapdataCategoryId = wsClient.doQuery(queryMapCategoryId);
                                JSONObject resultCategoryId = (JSONObject)parser.parse(mapdataCategoryId.get(0).toString());
                                JSONObject contentjsonCategoryId = (JSONObject)parser.parse(resultCategoryId.get("contentjson").toString());
                                JSONObject fieldsFiliali = (JSONObject)parser.parse(contentjsonCategoryId.get("fields").toString());
                                JSONArray fields_arrayFiliali = (JSONArray) fieldsFiliali.get("field");
                                for (Object field: fields_arrayFiliali) {
                                    JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                    JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                    recordFieldCategoryId.put(((JSONObject)field).get("fieldname").toString(), categorieMerceologicheObject.get(originalFiled.get("OrgfieldName").toString()));
                                }
                                // System.out.println("RECORD PRODUCT CATEGORY");
                                // System.out.println(recordFieldCategoryId);

                                recordFieldCategoryId.put("assigned_user_id", wsClient.getUserID());
                                recordMapCategoryId.put("elementType", "cbproductcategory");
                                recordMapCategoryId.put("element", Util.getJson(recordFieldCategoryId));
                                recordMapCategoryId.put("searchOn", "categorysrcid");
                                StringBuilder builderRemoveIndexZero = new StringBuilder(recordFieldCategoryId.keySet().toString());
                                builderRemoveIndexZero.deleteCharAt(0);
                                StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                String updatedfields = builderRemoveIndexLast.toString();
                                recordMapCategoryId.put("updatedfields", updatedfields);
                                // System.out.println(recordMapCategoryId);
                                Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapCategoryId, "POST");
                                JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                                if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                                    recordField.put("linktocategory", obj.get("id").toString());
                                }
                            }
                        }
                    }
                }
            }
        }

        recordField.put("assigned_user_id", wsClient.getUserID());
        recordMap.put("elementType", fieldname);
        recordMap.put("element", Util.getJson(recordField));
        recordMap.put("searchOn", fieldToSearch);
        StringBuilder builderRemoveIndexZero = new StringBuilder(recordField.keySet().toString());
        builderRemoveIndexZero.deleteCharAt(0);
        StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
        builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
        String updatedfields = builderRemoveIndexLast.toString();
        recordMap.put("updatedfields", updatedfields);
        // System.out.println(recordMap);
        return recordMap;
    }

    private void createRecordsInMap(Map<String, String> moduleCRMID) throws ParseException {
        for (Map<String, Object> record: lastRecordToCreate
             ) {
            String module = record.get("elementType").toString();
            Map<String, String> uitype10fields = getUIType10Field(module);
            JSONParser parser = new JSONParser();
            JSONObject recordFields = (JSONObject) parser.parse(record.get("element").toString());
            // System.out.println("Element:: " + recordFields);
            // System.out.println("Uitype10fields:: " + uitype10fields);
            // System.out.println("Module CRMID:: " + moduleCRMID);
            for (Object key : uitype10fields.keySet()) {
                String keyStr = (String)key;
                if (moduleCRMID.containsKey(uitype10fields.get(keyStr))) {
                    // set the field value
                    recordFields.put(keyStr, moduleCRMID.get(uitype10fields.get(keyStr)));
                } else {
                    // System.out.println("SEMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
                    // System.out.println("Module:: " + module);
                    // System.out.println("record:: " + record);
                    // System.out.println("key:: " + keyStr);
                    // System.out.println(recordFields.containsKey(keyStr));
                    // System.out.println(recordFields.get(keyStr).toString().equals(""));
                    // System.out.println( recordFields.get(keyStr).toString());
                    if (recordFields.containsKey(keyStr) && recordFields.get(keyStr) != null &&
                            recordFields.get(keyStr) != "") {

                        // System.out.println(recordFields.containsKey(keyStr));
                        // System.out.println(recordFields.get(keyStr));
                        // System.out.println( recordFields.get(keyStr).toString());
                        // TODO: 4/10/20 Scenario for Prodotti
                        // System.out.println("SUKARIIIIIIIIIIII YA WALEBOOOOOOOOOOOOOOOOOOOOO");
                        // Get field to search when we want to create module record
                        Map<String, Object> fieldToSearch = getSearchField(module);
                        // System.out.println("search:: " + uitype10fields.get(keyStr));
                        // System.out.println("search:: " + recordFields.get(keyStr));
                        // System.out.println("search:: " + fieldToSearch.get(keyStr));


                        if (recordFields.get(keyStr).toString().contains("x")) {
                            recordFields.put(keyStr, recordFields.get(keyStr));
                        } else {
                            Map<String, Object> searchResult = searchRecord(uitype10fields.get(keyStr),
                                    String.valueOf(recordFields.get(keyStr)), fieldToSearch.get(keyStr).toString(),
                                    "", false);
                            if (((boolean) searchResult.get("status")) && !((boolean) searchResult.get("mustbeupdated"))) {
                                recordFields.put(keyStr, searchResult.get("crmid"));
                            }
                        }
                    }
                }
            }
            record.put("element", Util.getJson(recordFields));
            System.out.println("Util.getJson(d) for Child Record = " + record);
            Object d = wsClient.doInvoke(Util.methodUPSERT, record, "POST");
            System.out.println("Util.getJson(d) for Child Record = " + Util.getJson(d));
        }

    }

    private Map<String, Object> searchByID(Object response, String id) throws ParseException {
        // System.out.println(response);
        // System.out.println(id);



        Map<String, Object> objValue = new HashMap<>();
        JSONParser parser = new JSONParser();
        JSONArray resArray = (JSONArray) parser.parse(response.toString());
        // System.out.println(resArray);
        for (Object object: resArray
             ) {
            JSONObject record = (JSONObject) parser.parse(object.toString());
            // System.out.println(record.get("ID").toString().equals(id));
            // System.out.println(id);
            if (record.get("ID").toString().equals(id)) {
                // System.out.println("KIGAGULAAAAAAAA");
                objValue = record;
                return objValue;
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
        // System.out.println(Arrays.toString(headersArray));
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
        // System.out.println("Util.getJson(d) = " + Util.getJson(d));
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