package consumer;

import helper.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import service.RESTClient;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

@SuppressWarnings("ALL")
public class UpdateConsumer extends Consumer {
    private final String topic = (Util.getProperty("kafka.topicname").isEmpty()) ?
            System.getenv("TOPIC_NAME") : Util.getProperty("kafka.topicname");
    private final String rest_api_url = (Util.getProperty("producer.fetch.url").isEmpty()) ?
            System.getenv("API_BASE_URL") : Util.getProperty("producer.fetch.url");
    public final String restAPIKey = (Util.getProperty("producer.fetch.url.apikey").isEmpty()) ?
            System.getenv("API_KEY") : Util.getProperty("producer.fetch.url.apikey");
    protected static final String useFieldMapping = (Util.getProperty("consumer.useFieldMapping").isEmpty()) ?
            System.getenv("USE_MAPPING_FLAG") : Util.getProperty("consumer.useFieldMapping");
    protected static final String fieldsDoQuery = (Util.getProperty("consumer.fieldDoQuery").isEmpty()) ?
            System.getenv("FIELD_DO_QUERY") : Util.getProperty("consumer.fieldDoQuery");
    protected static final String mainModuleToSync = (Util.getProperty("corebos.sync.module").isEmpty()) ?
            System.getenv("COREBOS_MODULE_TO_SYNC") : Util.getProperty("corebos.sync.module");
    private String identifier = (Util.getProperty("consumer.identity").isEmpty()) ?
            System.getenv("CONSUMER_IDENTIFIER") : Util.getProperty("consumer.identity");

    private ArrayList<Map<String, Object>> lastRecordToCreate = new ArrayList<>();
    private Map<String, String> uitype10fields = new HashMap<>();
    private Map<String, String> moduleDateFields = new HashMap<>();
    protected RESTClient restClient;
    RebalanceListner rebalanceListner;

    public UpdateConsumer() throws Exception {
        List topics = new ArrayList();
        topics.add(topic);
        restClient =new RESTClient(rest_api_url);
        rebalanceListner = new RebalanceListner(kafkaConsumer);
        kafkaConsumer.subscribe(topics, rebalanceListner);
    }

    public void init() {
        try {
            while (true) {
                //System.out.println("************************************BENCHMARK**************************************");
                ConsumerRecords records = kafkaConsumer.poll(Duration.ofMillis(3000));
                //System.out.println("TOTAL RECORD IN POLL:: " + records.count());

                long startTimeToProcessAllRecord = System.currentTimeMillis();
                for (Object o : records) {
                    //long startTimeToProcessRecord = System.currentTimeMillis();
                    ConsumerRecord record = (ConsumerRecord) o;
                    readRecord(record);
                    //long timeElapsedToProcessRecord = System.currentTimeMillis() - startTimeToProcessRecord;
                    //System.out.println("TIME TO PROCESS SINGLE RECORD:: " + ( timeElapsedToProcessRecord / 1000 ) + " seconds");
                    rebalanceListner.setCurrentOffsets(record.topic(), record.partition(), record.offset());
                }
                //long timeElapsedToProcessAllRecord = System.currentTimeMillis() - startTimeToProcessAllRecord;
                //System.out.println("TIME TO PROCESS RECORDS IN POLL:: " + ( timeElapsedToProcessAllRecord / 1000 ) + "seconds");
                if (!records.isEmpty()) {
                    kafkaConsumer.commitSync(rebalanceListner.getCurrentOffsets());
                }
                //System.out.println("******************************************************************************");
                //System.out.println("");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }


    private void readRecord(ConsumerRecord record) throws Exception {
        System.out.println(String.format("Topic - %s, Key - %s, Partition - %d, Value: %s", record.topic(), record.key(),record.partition(), record.value()));
        JSONParser jsonParserX = new JSONParser();
        JSONObject objectValue = (JSONObject) jsonParserX.parse(record.value().toString());
        String operation = objectValue.get("operation").toString();
        if (operation.equals(Util.methodUPDATE) && !identifier.isEmpty() && identifier.equals("shipment")) {
            Object shipment = Util.getObjectFromJson(objectValue.get("shipment").toString(), Object.class);
            Object shipmentStatus = Util.getObjectFromJson(objectValue.get("status").toString(), Object.class);
            System.out.println("Upserting the Record for Shipment Module");
            lastRecordToCreate.clear();
            upsertRecord(mainModuleToSync, (Map) shipment, (Map) shipmentStatus);
        } else if (operation.equals(Util.methodUPDATE) && !identifier.isEmpty() && identifier.equals("viaggi")) {
            Object viaggi = Util.getObjectFromJson(objectValue.get("viaggi").toString(), Object.class);
            System.out.println("Upserting the Record for Viaggi Module");
            /*
            * We never use this Filled for Our Viaggi Module
            **/
            lastRecordToCreate.clear();
            upsertRecord(mainModuleToSync, (Map) viaggi, null);
        } else if (operation.equals(Util.methodUPDATE) && !identifier.isEmpty() && identifier.equals("danni")) {
            Object danni = Util.getObjectFromJson(objectValue.get("danni").toString(), Object.class);
            System.out.println("Upserting the Record for Danni Module");
            /*
             * We never use this Filled for Our Viaggi Module
             **/
            lastRecordToCreate.clear();
            upsertRecord(mainModuleToSync, (Map) danni, null);
        }
    }

    private void updateShipmentsStatus(String module, Map message) throws Exception {
        //System.out.println("PROCESSING SHIPMENT STATUSES");
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
                    // https://dzone.com/articles/guava-splitter-vs-stringutils
                    String[] statusArray = StringUtils.split(v, "#");
                    String statusLatestDate = "";
                    String latestStatus = "";
                    String linkToStatusCRMID = "";
                    String latestStatusShipmentKey = "";
                    for (String statusChanges : statusArray) {
                        // We have to Use Google guava
                        // https://dzone.com/articles/guava-splitter-vs-stringutils
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
                            if (processedMessageData.get("linktostatus") != null) {
                                linkToStatusCRMID = processedMessageData.get("linktostatus").toString();
                            }
                            latestStatusShipmentKey = k;
                        } else {
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

                        Map<String, Object> searchProcessLog = searchRecord(module, "", "",
                                queryCondition.toString(), false);
                        if (!((boolean) searchProcessLog.get("status"))) {
                            StringBuilder mapName, condition, queryMap;
                            mapName = new StringBuilder("REST2").append(module);
                            String mapModule = "cbMap";
                            condition = new StringBuilder("mapname").append("='").append(mapName).append("'");
                            queryMap = new StringBuilder("select * from ").append(mapModule).append(" where ").append(condition);

                            JSONArray mapdata = wsClient.doQuery(queryMap.toString(), 3);
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
                            //mapToSend.put("searchOn", "linktoshipments");
                            //StringBuilder builderRemoveIndexZero = new StringBuilder(fieldUpdate.keySet().toString());
                            //builderRemoveIndexZero.deleteCharAt(0);
                            //StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                            //builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                            //String updatedfields = builderRemoveIndexLast.toString();
                            //mapToSend.put("updatedfields", updatedfields);
                            //System.out.println(mapToSend);

                            Object d = wsClient.doInvoke(Util.methodCREATE, mapToSend, "POST", 3);
                        }
                    }
                }
            }
        }
    }

    private void upsertRecord(String module, Map element, Map shipmentStatus) throws Exception {
        Map<String, Object> mapToSend = new HashMap<>();
        Map<String, Object> fieldUpdate = new HashMap<>();

        if (Objects.equals(useFieldMapping, "yes")) {
            // Module Special field e.g Date, Datetime
            getModuleDateFields(module);
            // Get All uitype 10 Module fields
            JSONObject module_info = wsClient.doDescribe(module, 3);
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
            StringBuilder mapName, mapModule, condition, queryMap;
            mapName = new StringBuilder("REST2").append(module);
            mapModule = new StringBuilder("cbMap");
            condition = new StringBuilder("mapname").append("='").append(mapName).append("'");
            queryMap = new StringBuilder("select * from ").append(mapModule).append(" where ").append(condition);
            JSONArray mapdata = wsClient.doQuery(queryMap.toString(), 3);
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
        }

        Object d = wsClient.doInvoke(Util.methodUPSERT, mapToSend, "POST", 3);
        /*
         * We Nee to Create Other Module Record which depend on this Created Record
         * **/
        Map<String, String> moduleCRMID = new HashMap<>();
        JSONParser parser = new JSONParser();
        JSONObject createdRecord = (JSONObject)parser.parse(Util.getJson(d));
        moduleCRMID.put(module, createdRecord.get("id").toString());
        createRecordsInMap(moduleCRMID);

        if (!identifier.isEmpty() && identifier.equals("shipment")) {
            updateShipmentsStatus("ProcessLog", shipmentStatus);
        }
    }

    @SuppressWarnings("unchecked")
    private Object getFieldValue(String orgfieldName, Map element, Map<String, String> moduleFieldInfo,
                                 String parentModule, String fieldname) throws Exception {
        JSONObject rs = new JSONObject();
        JSONObject record = new JSONObject();
        record.putAll(element);
        System.out.println("Object Key:: " + orgfieldName);
        System.out.println("Module Field:: " + fieldname);
        if(record.containsKey(orgfieldName) || orgfieldName.equals("distribuzioneFornitoreId") ||
                orgfieldName.equals("raeeFornitoreId")) {
            /*
            1. Check if the field value can be converted to JSONArray or JSONObject
            2. If is Object or JSON Array check if it exit in Module field search and is one of the module field
            3. if the above statement if false means the JSONArray or JSONObject record depend on the Main Module Record which is about to be created
            */

            String jsonValue = "";
            if (orgfieldName.equals("distribuzioneFornitoreId") || orgfieldName.equals("raeeFornitoreId")) {
                jsonValue = "{}";
            } else {
                jsonValue = Util.getJson(record.get(orgfieldName));
            }

            JSONParser parser = new JSONParser();
            if ((parser.parse(jsonValue) instanceof JSONObject) ||  orgfieldName.equals("distribuzioneFornitoreId") ||
                    orgfieldName.equals("raeeFornitoreId")) {

                if (orgfieldName.equals("indirizzo")) {
                    JSONObject indirizzo = null;
                    if (record.containsKey("indirizzo") && record.get("indirizzo") != null) {
                        indirizzo = (JSONObject) parser.parse(Util.getJson(record.get("indirizzo")));
                        if (indirizzo == null || !indirizzo.containsKey("comune") || indirizzo.get("comune") == null) {
                            rs.put("status", "notfound");
                            rs.put("value",  "");
                            return rs;
                        } else {
                            if (indirizzo.get("comune") == null) {
                                rs.put("status", "notfound");
                                rs.put("value",  "");
                                return rs;
                            } else {
                                if (fieldname.equals("comune_text")) {
                                    rs.put("status", "found");
                                    rs.put("value",  indirizzo.get("comune").toString());
                                    return rs;
                                } else {
                                    Map<String, Object> searchResultGeoboundary = searchRecord("Geoboundary",
                                            indirizzo.get("comune").toString(), "geoname", "", false);
                                    if (((boolean) searchResultGeoboundary.get("status"))) {
                                        rs.put("status", "found");
                                        rs.put("value",  searchResultGeoboundary.get("crmid"));
                                        return rs;
                                    } else {
                                        rs.put("status", "notfound");
                                        rs.put("value",  "");
                                        return rs;
                                    }
                                }
                            }
                        }
                    }

                }

                if (orgfieldName.equals("tipologiaDanno") && !identifier.isEmpty() && identifier.equals("danni")) {
                    JSONObject tipologiaDannoObject = null;
                    if (record.containsKey("tipologiaDanno") && record.get("tipologiaDanno") != null) {
                        tipologiaDannoObject = (JSONObject) parser.parse(Util.getJson(record.get("tipologiaDanno")));
                        String value = tipologiaDannoObject.get("descrizione").toString();
                        rs.put("status", "found");
                        rs.put("value",  value);
                        return rs;
                    } else {
                        rs.put("status", "notfound");
                        rs.put("value",  "");
                        return rs;
                    }

                }

                if (orgfieldName.equals("causaDanno") && !identifier.isEmpty() && identifier.equals("danni")) {
                    JSONObject causaDannoObject = null;
                    if (record.containsKey("causaDanno") && record.get("causaDanno") != null) {
                        causaDannoObject = (JSONObject) parser.parse(Util.getJson(record.get("causaDanno")));
                        String value = causaDannoObject.get("descrizione").toString();
                        rs.put("status", "found");
                        rs.put("value",  value);
                        return rs;
                    } else {
                        rs.put("status", "notfound");
                        rs.put("value",  "");
                        return rs;
                    }

                }

                // Get the Search fields
                 Map<String, String> fieldToSearch = getSearchField(parentModule);
                 if (!fieldToSearch.isEmpty() && moduleFieldInfo.containsKey(fieldname)) {
                     String searchID = "";
                     if (orgfieldName.equals("distribuzioneFornitoreId") || orgfieldName.equals("raeeFornitoreId")) {
                         JSONObject importoSpedizione = null;
                         if (record.containsKey("importoSpedizione") && record.get("importoSpedizione") != null) {
                             importoSpedizione = (JSONObject) parser.parse(Util.getJson(record.get("importoSpedizione")));
                         }

                         if (importoSpedizione == null) {
                             rs.put("status", "notfound");
                             rs.put("value",  "");
                             return rs;
                         }
                         if (orgfieldName.equals("distribuzioneFornitoreId")) {
                             if (importoSpedizione.containsKey("distribuzioneFornitoreId") && importoSpedizione.get("distribuzioneFornitoreId") != null) {
                                 searchID = importoSpedizione.get("distribuzioneFornitoreId").toString();
                             } else {
                                 rs.put("status", "notfound");
                                 rs.put("value",  "");
                                 return rs;
                             }

                         } else {
                             if (importoSpedizione.containsKey("raeeFornitoreId") && importoSpedizione.get("raeeFornitoreId") != null) {
                                 searchID = importoSpedizione.get("raeeFornitoreId").toString();
                             } else {
                                 rs.put("status", "notfound");
                                 rs.put("value",  "");
                                 return rs;
                             }
                         }
                     } else {
                         searchID = ((JSONObject) parser.parse(jsonValue)).get("ID").toString();
                     }

                     Map<String, Object> searchResult = null;
                     if (moduleFieldInfo.get(fieldname).equals("Services")) {
                         if (fieldToSearch.containsKey(orgfieldName) && fieldToSearch.get(orgfieldName) != null && !fieldToSearch.get(orgfieldName).isEmpty()) {
                             searchResult = searchRecord(moduleFieldInfo.get(fieldname),
                                     searchID, fieldToSearch.get(orgfieldName).toString(), "", false);
                         }

                     } else {
                         if (fieldToSearch.containsKey(orgfieldName) && fieldToSearch.get(orgfieldName) != null && !fieldToSearch.get(orgfieldName).isEmpty()) {
                             searchResult = searchRecord(moduleFieldInfo.get(fieldname),
                                     searchID, fieldToSearch.get(orgfieldName).toString(), "", true);
                         }
                     }
                     if ( searchResult != null && ((boolean) searchResult.get("status")) && !((boolean) searchResult.get("mustbeupdated"))) {
                         rs.put("status", "found");
                         rs.put("value",  searchResult.get("crmid"));
                     } else {
                         // for Special field key distribuzioneFornitore Id, raeeFornitoreId
                         if (orgfieldName.equals("distribuzioneFornitoreId") || orgfieldName.equals("raeeFornitoreId")) {
                                 if (((JSONObject) parser.parse(jsonValue)).get("importoSpedizione") != null) {
                                     String endpoint = "fornitori";
                                     //String objectKey = "fornitori";
                                     String objectKey = "fornitore";
                                     String id = null;
                                     JSONObject importoSpedizione = (JSONObject) parser.parse(Util.getJson(record.get("importoSpedizione")));
                                     if (orgfieldName.equals("distribuzioneFornitoreId") && importoSpedizione.get("distribuzioneFornitoreId") != null) {
                                         id = importoSpedizione.get("distribuzioneFornitoreId").toString();
                                     } else if(fieldname.equals("raeeFornitoreId") && importoSpedizione.get("raeeFornitoreId") != null) {
                                         id = importoSpedizione.get("raeeFornitoreId").toString();
                                     }

                                     endpoint = endpoint + "/" + id;
                                     Map<String, Object> fornitoriResponse = doGetJSONObject(restAPIKey, endpoint, objectKey);
                                     if (fornitoriResponse == null) {
                                         rs.put("status", "notfound");
                                         rs.put("value",  "");
                                         return rs;
                                     }

                                     //Map<String, Object> fornitoriObject = searchByID(fornitoriResponse, id);
                                     Map<String, Object> fornitoriObject = fornitoriResponse;
                                     StringBuilder mapName, mapModule, condition, queryMap;
                                     mapName = new StringBuilder(orgfieldName).append("2").append(fieldname);
                                     mapModule = new StringBuilder("cbMap");
                                     condition = new StringBuilder("mapname").append("='").append(mapName).append("'");
                                     queryMap = new StringBuilder("select * from ").append(mapModule).append(" where ").append(condition);
                                     // Map to Create the Record
                                     Map<String, Object> recordMap = new HashMap<>();
                                     Map<String, Object> recordField = new HashMap<>();
                                     JSONArray mapdata = wsClient.doQuery(queryMap.toString(), 3);
                                     JSONObject result = (JSONObject)parser.parse(mapdata.get(0).toString());
                                     JSONObject contentjson = (JSONObject)parser.parse(result.get("contentjson").toString());
                                     JSONObject fields = (JSONObject)parser.parse(contentjson.get("fields").toString());
                                     JSONArray fields_array = (JSONArray) fields.get("field");
                                     for (Object field: fields_array) {
                                         JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                         JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                             recordField.put(((JSONObject)field).get("fieldname").toString(), fornitoriObject.get(originalFiled.get("OrgfieldName").toString()));
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
                                     Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMap, "POST", 3);
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
                         } else {
                             // We Have to Create the New Record and get Its CRMID
                             Map<String, Object> recordMap = new HashMap<>();
                             Map<String, Object> recordField = new HashMap<>();
                             // 1. Get Map for Adding that Module from Rest API
                             StringBuilder mapName, condition, queryMap;
                             mapName = new StringBuilder(orgfieldName).append("2").append(moduleFieldInfo.get(fieldname));
                             String mapModule = "cbMap";
                             condition = new StringBuilder("mapname").append("='").append(mapName).append("'");
                             queryMap = new StringBuilder("select * from ").append(mapModule).append(" where ").append(condition);
                             JSONArray mapdata = wsClient.doQuery(queryMap.toString(), 3);
                             JSONObject result = (JSONObject)parser.parse(mapdata.get(0).toString());
                             JSONObject contentjson = (JSONObject)parser.parse(result.get("contentjson").toString());
                             JSONObject fields = (JSONObject)parser.parse(contentjson.get("fields").toString());
                             JSONArray fields_array = (JSONArray) fields.get("field");
                             for (Object field: fields_array) {
                                 JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                 JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                 Object value = getFieldValue(originalFiled.get("OrgfieldName").toString(), (Map) record.get(orgfieldName),
                                         moduleFieldInfo, parentModule, fieldname);
                                 if (((JSONObject) value).get("status").toString().equals("found")) {
                                     if (moduleDateFields.containsKey(fieldname) &&
                                             (moduleDateFields.get(fieldname).equals("5") ||
                                                     moduleDateFields.get(fieldname).equals("50"))) {
                                         String dateValue = ((JSONObject)value).get("value").toString().replace("T", " ");
                                         recordField.put(((JSONObject)field).get("fieldname").toString(), dateValue);
                                     } else {
                                         if (((JSONObject) field).get("fieldname").toString().equals("entrusting_date") || ((JSONObject) field).get("fieldname").toString().equals("bookingdatetime") ||
                                                 ((JSONObject) field).get("fieldname").toString().equals("chosenappointstart") || ((JSONObject) field).get("fieldname").toString().equals("chosenappointfinish") ||
                                                 ((JSONObject) field).get("fieldname").toString().equals("scheduledstart") || ((JSONObject) field).get("fieldname").toString().equals("scheduledfinish")) {

                                             if (((JSONObject)value).get("value") != null) {
                                                 String dateValue = ((JSONObject)value).get("value").toString().replace("T", " ");
                                                 recordField.put(((JSONObject)field).get("fieldname").toString(), dateValue);
                                             } else {
                                                 recordField.put(((JSONObject)field).get("fieldname").toString(), ((JSONObject)value).get("value"));
                                             }
                                         } else {
                                             recordField.put(((JSONObject)field).get("fieldname").toString(), ((JSONObject)value).get("value"));
                                         }
                                     }
                                 }
                             }


                             // Handle Special Case for indirizzoMittente, indirizzoRitiro, indirizzoConsegna, indirizzoDestinatario
                             if (orgfieldName.equals("indirizzoMittente") || orgfieldName.equals("indirizzoRitiro") ||
                                     orgfieldName.equals("indirizzoConsegna") || orgfieldName.equals("indirizzoDestinatario")) {
                                 /*
                                  * Query Geoboundary Module  where geoname == comune
                                  */
                                 Map<String, Object> searchResultGeoboundary = null;
                                 if (((JSONObject) parser.parse(jsonValue)).containsKey("comune") && ((JSONObject) parser.parse(jsonValue)).get("comune") != null &&
                                         !((JSONObject) parser.parse(jsonValue)).get("comune").toString().isEmpty()) {
                                     searchResultGeoboundary = searchRecord("Geoboundary",
                                             ((JSONObject) parser.parse(jsonValue)).get("comune").toString(),
                                             "geoname", "", false);
                                 } else {
                                     searchResultGeoboundary = new HashMap<>();
                                     searchResultGeoboundary.put("status", false);
                                     searchResultGeoboundary.put("crmid", "");
                                     searchResultGeoboundary.put("mustbeupdated", false);
                                 }

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

                             if (orgfieldName.equals("ritiro")) {
                                 /*
                                  * Query cbCompany module in order to check whether there already exists a record where branchsrcid == filialeId.
                                  */
                                 Map<String, Object> searchResultCompany = null;
                                 if (((JSONObject) parser.parse(jsonValue)).containsKey("filialeId") &&
                                         ((JSONObject) parser.parse(jsonValue)).get("filialeId") != null &&
                                         !((JSONObject) parser.parse(jsonValue)).get("filialeId").toString().isEmpty()) {
                                     searchResultCompany = searchRecord("cbCompany",
                                             ((JSONObject) parser.parse(jsonValue)).get("filialeId").toString(),
                                             "branchsrcid", "", false);
                                 } else {
                                     searchResultCompany = new HashMap<>();
                                     searchResultCompany.put("status", false);
                                     searchResultCompany.put("crmid", "");
                                     searchResultCompany.put("mustbeupdated", false);
                                 }

                                 if (((boolean) searchResultCompany.get("status")) && !((boolean) searchResultCompany.get("mustbeupdated"))) {
                                     Map<String, String> referenceFields = getUIType10Field(moduleFieldInfo.get(fieldname));
                                     for (Object key : referenceFields.keySet()) {
                                         String keyStr = (String)key;
                                         if (referenceFields.get(keyStr).equals("cbCompany")) {
                                             recordField.put(keyStr, searchResultCompany.get("crmid"));
                                         }
                                     }

                                 } else {
                                     /*
                                      * Query cbCompany module in order to check whether there already exists a record where branchsrcid == filialeId.
                                      */
                                         if (parser.parse(jsonValue) != null) {
                                             String endpoint = "filiali";
                                             //String objectKey = "filiali";
                                             String objectKey = "filiale";
                                             JSONObject ritiro = (JSONObject) parser.parse(jsonValue);
                                             String id = null;
                                             Map<String, Object> filialiObject = null;

                                             if (ritiro.containsKey("filialeId") && ritiro.get("filialeId") != null) {
                                                 id = ritiro.get("filialeId").toString();
                                                 endpoint = endpoint + "/" + id;
                                                 filialiObject = doGetJSONObject(restAPIKey, endpoint, objectKey);
                                             }

                                             if (filialiObject != null) {
                                                 //Map<String, Object> filialiObject = searchByID(filialiResponse, id);
                                                 //Map<String, Object> filialiObject = filialiResponse;
                                                 if (!filialiObject.isEmpty()) {
                                                     Map<String, Object> recordMapFiliali = new HashMap<>();
                                                     Map<String, Object> recordFieldFiliali = new HashMap<>();
                                                     StringBuilder conditionFiliali, queryMapFiliali;
                                                     String mapNameFiliali = "filialeId2cbCompany";
                                                     String mapModuleFiliali = "cbMap";
                                                     conditionFiliali = new StringBuilder("mapname").append("='").append(mapNameFiliali).append("'");
                                                     queryMapFiliali = new StringBuilder("select * from ").append(mapModuleFiliali).append(" where ").append(conditionFiliali);
                                                     JSONArray mapdataFiliali = wsClient.doQuery(queryMapFiliali.toString(), 3);
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
                                                     if (((JSONObject) parser.parse(filialiObject.toString())).containsKey("comune") && ((JSONObject) parser.parse(filialiObject.toString())).get("comune") != null &&
                                                             !((JSONObject) parser.parse(filialiObject.toString())).get("comune").toString().isEmpty()) {
                                                         Map<String, Object> searchResultGeoboundary = searchRecord("Geoboundary",
                                                                 ((JSONObject) parser.parse(filialiObject.toString())).get("comune").toString(),
                                                                 "geoname", "", false);
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

                                                     Map<String, Object> searchResultVendorModule;
                                                     /*
                                                     * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                                                     * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                                                     * vettoreId
                                                     * */
                                                     if (((JSONObject) parser.parse(filialiObject.toString())).containsKey("vettoreId") &&
                                                             ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId") != null &&
                                                             !((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString().isEmpty()) {
                                                         searchResultVendorModule = searchRecord("Vendors",
                                                                 ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString(),
                                                                 "suppliersrcid", "Vettore", false);
                                                     } else {
                                                         searchResultVendorModule = new HashMap<>();
                                                         searchResultVendorModule.put("status", false);
                                                         searchResultVendorModule.put("crmid", "");
                                                         searchResultVendorModule.put("mustbeupdated", false);
                                                     }

                                                     if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                                         recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                                     } else {
                                                             String vettoriEndpoint = "vettori";
                                                             String vettoriDataKey = "vettori";
                                                             Object vettoriResponse = null;
                                                             if (filialiObject.containsKey("vettoreId") && filialiObject.get("vettoreId") != null) {
                                                                 vettoriResponse = doGet(restAPIKey, vettoriEndpoint, vettoriDataKey);
                                                             }

                                                             if (vettoriResponse != null) {
                                                                 Map<String, Object> vettoriObject = searchByID(vettoriResponse,
                                                                         ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString());
                                                                 if (!vettoriObject.isEmpty()) {
                                                                     Map<String, Object> vettoriRecordMap = new HashMap<>();
                                                                     Map<String, Object> vettoriRecordField = new HashMap<>();
                                                                     StringBuilder vettoriCondition, vettoriQueryMap;
                                                                     String vettoriMapName = "vettoreId2Vendors";
                                                                     String vettoriMapModule = "cbMap";
                                                                     vettoriCondition = new StringBuilder("mapname").append("='").append(vettoriMapName).append("'");
                                                                     vettoriQueryMap = new StringBuilder("select * from ").append(vettoriMapModule).append(" where ").append(vettoriCondition);
                                                                     JSONArray vettoriMapData = wsClient.doQuery(vettoriQueryMap.toString(), 3);
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
                                                                     Object newRecord = wsClient.doInvoke(Util.methodUPSERT, vettoriRecordMap, "POST", 3);
                                                                     JSONObject vettoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                                     if (vettoriobjrec.containsKey("id") && !vettoriobjrec.get("id").toString().equals("")) {
                                                                         recordFieldFiliali.put("linktocarrier", vettoriobjrec.get("id").toString());
                                                                     }
                                                                 }
                                                             }
                                                     }

                                                     /*
                                                      * Query Vendors module in order to check whether there already exists a record where suppliersrcid == fornitoreId AND type == 'Fornitore'.
                                                      * If there exists none, then call the api/fornitori endpoint and retrieve the object where ID==fornitoreId. Then, create a new Vendor in CoreBOS
                                                      * fornitoreId
                                                      * */
                                                     if (((JSONObject) parser.parse(filialiObject.toString())).containsKey("fornitoreId") &&
                                                             ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId") != null &&
                                                             !((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString().isEmpty()) {
                                                         searchResultVendorModule = searchRecord("Vendors",
                                                                 ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString(),
                                                                 "suppliersrcid", "Fornitore", false);
                                                     } else {
                                                         searchResultVendorModule = new HashMap<>();
                                                         searchResultVendorModule.put("status", false);
                                                         searchResultVendorModule.put("crmid", "");
                                                         searchResultVendorModule.put("mustbeupdated", false);
                                                     }

                                                     if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                                         recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                                     } else {
                                                             String fornitoriEndpoint = "fornitori";
                                                             //String fornitoriDataKey = "fornitori";
                                                             String fornitoriDataKey = "fornitore";

                                                         Map<String, Object> fornitoriResponse = null;

                                                         if (filialiObject.containsKey("fornitoreId") && filialiObject.get("fornitoreId") != null) {
                                                             fornitoriEndpoint = fornitoriEndpoint + "/" + ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString();
                                                             fornitoriResponse = doGetJSONObject(restAPIKey, fornitoriEndpoint, fornitoriDataKey);
                                                         }

                                                         if (fornitoriResponse != null) {
                                                             //Map<String, Object> fornitoriObject = searchByID(fornitoriResponse, ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString());
                                                             Map<String, Object> fornitoriObject = fornitoriResponse;
                                                             if (!fornitoriObject.isEmpty()) {
                                                                 Map<String, Object> fornitoriRecordMap = new HashMap<>();
                                                                 Map<String, Object> fornitoriRecordField = new HashMap<>();
                                                                 StringBuilder fornitoriCondition, fornitoriQueryMap;
                                                                 String fornitoriMapName = "fornitoreId2Vendors";
                                                                 String fornitoriMapModule = "cbMap";
                                                                 fornitoriCondition = new StringBuilder("mapname").append("='").append(fornitoriMapName).append("'");
                                                                 fornitoriQueryMap = new StringBuilder("select * from ").append(fornitoriMapModule).append(" where ").append(fornitoriCondition);
                                                                 JSONArray fornitoriMapData = wsClient.doQuery(fornitoriQueryMap.toString(), 3);
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
                                                                 fornitoriRecordField.put("type", "Fornitore");
                                                                 fornitoriRecordMap.put("elementType", "Vendors");
                                                                 fornitoriRecordMap.put("element", Util.getJson(fornitoriRecordField));
                                                                 fornitoriRecordMap.put("searchOn", "suppliersrcid");
                                                                 StringBuilder builderRemoveIndexZero = new StringBuilder(fornitoriRecordField.keySet().toString());
                                                                 builderRemoveIndexZero.deleteCharAt(0);
                                                                 StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                                                 builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                                                 String updatedfields = builderRemoveIndexLast.toString();
                                                                 fornitoriRecordMap.put("updatedfields", updatedfields);
                                                                 Object newRecord = wsClient.doInvoke(Util.methodUPSERT, fornitoriRecordMap, "POST", 3);
                                                                 JSONObject fornitoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                                 if (fornitoriobjrec.containsKey("id") && !fornitoriobjrec.get("id").toString().equals("")) {
                                                                     recordFieldFiliali.put("vendorid", fornitoriobjrec.get("id").toString());
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
                                                     Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapFiliali, "POST", 3);
                                                     JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                     if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                                                         recordField.put("branchid", obj.get("id").toString());
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
                                 Map<String, Object> searchResultDeliveryAreas;
                                 if (((JSONObject) parser.parse(jsonValue)).containsKey("ID") &&
                                         ((JSONObject) parser.parse(jsonValue)).get("ID") != null &&
                                         !((JSONObject) parser.parse(jsonValue)).get("ID").toString().isEmpty()) {
                                     searchResultDeliveryAreas = searchRecord("DeliveryAreas",
                                             ((JSONObject) parser.parse(jsonValue)).get("ID").toString(),
                                             "areasrcid", "", false);
                                 } else {
                                     searchResultDeliveryAreas = new HashMap<>();
                                     searchResultDeliveryAreas.put("status", false);
                                     searchResultDeliveryAreas.put("crmid", "");
                                     searchResultDeliveryAreas.put("mustbeupdated", false);
                                 }


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
                                         if (parser.parse(jsonValue) != null) {
                                             String endpoint = "filiali";
                                             //String objectKey = "filiali";
                                             String objectKey = "filiale";
                                             JSONObject zonaConsegna = (JSONObject) parser.parse(jsonValue);
                                             String id = null;
                                             Map<String, Object> filialiResponse = null;
                                             if (zonaConsegna.containsKey("filialeId") && zonaConsegna.get("filialeId") != null) {
                                                 id = zonaConsegna.get("filialeId").toString();
                                                 endpoint = endpoint + "/" + id;
                                                 filialiResponse = doGetJSONObject(restAPIKey, endpoint, objectKey);
                                             }

                                             if (filialiResponse != null) {
                                                 //Map<String, Object> filialiObject = searchByID(filialiResponse, id);
                                                 Map<String, Object> filialiObject = filialiResponse;
                                                 if (!filialiObject.isEmpty()) {
                                                     Map<String, Object> recordMapFiliali = new HashMap<>();
                                                     Map<String, Object> recordFieldFiliali = new HashMap<>();
                                                     StringBuilder conditionFiliali, queryMapFiliali;
                                                     String mapNameFiliali = "filialeId2cbCompany";
                                                     String mapModuleFiliali = "cbMap";
                                                     conditionFiliali = new StringBuilder("mapname").append("='").append(mapNameFiliali).append("'");
                                                     queryMapFiliali = new StringBuilder("select * from ").append(mapModuleFiliali).append(" where ").append(conditionFiliali);
                                                     JSONArray mapdataFiliali = wsClient.doQuery(queryMapFiliali.toString(), 3);
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
                                                     if (filialiObject.get("comune") != null && !filialiObject.get("comune").toString().isEmpty()) {
                                                         Map<String, Object> searchResultGeoboundary = searchRecord(
                                                                 "Geoboundary", filialiObject.get("comune").toString(),
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

                                                     if (filialiObject.containsKey("vettoreId") && filialiObject.get("vettoreId") != null) {
                                                         /*
                                                          * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                                                          * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                                                          * vettoreId
                                                          * */
                                                         searchResultVendorModule = searchRecord("Vendors",
                                                                 filialiObject.get("vettoreId").toString(),
                                                                 "suppliersrcid", "Vettore", false);

                                                         if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                                             recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                                         } else {
                                                                 String vettoriEndpoint = "vettori";
                                                                 String vettoriDataKey = "vettori";
                                                                 Object vettoriResponse = null;
                                                                 if(filialiObject.containsKey("vettoreId") && filialiObject.get("vettoreId") != null) {
                                                                     vettoriResponse = doGet(restAPIKey, vettoriEndpoint, vettoriDataKey);
                                                                 }

                                                                 if (vettoriResponse != null) {
                                                                     Map<String, Object> vettoriObject = searchByID(vettoriResponse,
                                                                             ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString());
                                                                     if (!vettoriObject.isEmpty()) {
                                                                         Map<String, Object> vettoriRecordMap = new HashMap<>();
                                                                         Map<String, Object> vettoriRecordField = new HashMap<>();
                                                                         StringBuilder vettoriCondition, vettoriQueryMap;
                                                                         String vettoriMapName = "vettoreId2Vendors";
                                                                         String vettoriMapModule = "cbMap";
                                                                         vettoriCondition =new StringBuilder("mapname").append("='").append(vettoriMapName).append("'");
                                                                         vettoriQueryMap = new StringBuilder("select * from ").append(vettoriMapModule).append(" where ").append(vettoriCondition);
                                                                         JSONArray vettoriMapData = wsClient.doQuery(vettoriQueryMap.toString(), 3);
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
                                                                         Object newRecord = wsClient.doInvoke(Util.methodUPSERT, vettoriRecordMap, "POST", 3);
                                                                         JSONObject vettoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                                         if (vettoriobjrec.containsKey("id") && !vettoriobjrec.get("id").toString().equals("")) {
                                                                             recordFieldFiliali.put("linktocarrier", vettoriobjrec.get("id").toString());
                                                                         }
                                                                     }
                                                                 }
                                                         }
                                                     }

                                                     if (filialiObject.containsKey("fornitoreId") && filialiObject.get("fornitoreId") != null) {
                                                         /*
                                                          * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                                                          * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                                                          * fornitoreId
                                                          * */
                                                         searchResultVendorModule = searchRecord("Vendors",
                                                                 filialiObject.get("fornitoreId").toString(),
                                                                 "suppliersrcid", "Fornitore", false);
                                                         if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                                             recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                                         } else {
                                                                 String fornitoriEndpoint = "fornitori";
                                                                 //String fornitoriDataKey = "fornitori";
                                                                 String fornitoriDataKey = "fornitore";

                                                                 Map<String, Object> fornitoriResponse = null;
                                                                 if(filialiObject.containsKey("fornitoreId") && filialiObject.get("fornitoreId") != null) {
                                                                     fornitoriEndpoint = fornitoriEndpoint + "/" + ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString();
                                                                     fornitoriResponse = doGetJSONObject(restAPIKey, fornitoriEndpoint, fornitoriDataKey);
                                                                 }

                                                                 if (fornitoriResponse != null) {
                                                                     //Map<String, Object> fornitoriObject = searchByID(fornitoriResponse, ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString());
                                                                     Map<String, Object> fornitoriObject = fornitoriResponse;
                                                                     if (!fornitoriObject.isEmpty()) {
                                                                         Map<String, Object> fornitoriRecordMap = new HashMap<>();
                                                                         Map<String, Object> fornitoriRecordField = new HashMap<>();
                                                                         StringBuilder fornitoriCondition, fornitoriQueryMap;
                                                                         String fornitoriMapName = "fornitoreId2Vendors";
                                                                         String fornitoriMapModule = "cbMap";
                                                                         fornitoriCondition = new StringBuilder("mapname").append("='").append(fornitoriMapName).append("'");
                                                                         fornitoriQueryMap = new StringBuilder("select * from ").append(fornitoriMapModule).append(" where ").append(fornitoriCondition);
                                                                         JSONArray fornitoriMapData = wsClient.doQuery(fornitoriQueryMap.toString(), 3);
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
                                                                         Object newRecord = wsClient.doInvoke(Util.methodUPSERT, fornitoriRecordMap, "POST", 3);
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
                                                     Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapFiliali, "POST", 3);
                                                     JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                     if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                                                         recordField.put("linktobranch", obj.get("id").toString());
                                                     }
                                                 }
                                             }
                                         }

                                     /* In zonaConsegna, there is an API parameter, called tecnicoId.
                                        Query Technicians module in order to check whether there already exists a record where techniciansrcid == zonaConsegna.tecnicoId. If there exists none, then make an HTTP request to GET /tecnici/{id} where id should be the value of zonaConsegna.tecnicoId.
                                        Afterwards, create a new Technicians record in CoreBOS with the following mapping:
                                        */
                                     if (((JSONObject) parser.parse(jsonValue)).containsKey("tecnicoId") && ((JSONObject) parser.parse(jsonValue)).get("tecnicoId") != null) {
                                         Map<String, Object> searchResultTechnicians = searchRecord("Technicians",
                                                 ((JSONObject) parser.parse(jsonValue)).get("tecnicoId").toString(),
                                                 "techniciansrcid", "", false);

                                         if (((boolean) searchResultTechnicians.get("status")) && !((boolean) searchResultTechnicians.get("mustbeupdated"))) {
                                             Map<String, String> referenceFields = getUIType10Field(moduleFieldInfo.get(fieldname));
                                             for (Object key : referenceFields.keySet()) {
                                                 String keyStr = (String)key;
                                                 if (referenceFields.get(keyStr).equals("DeliveryAreas")) {
                                                     recordField.put(keyStr, searchResultTechnicians.get("crmid"));
                                                 }
                                             }

                                         } else {
                                                 String endpoint = "tecnici";
                                                 String objectKey = "tecnico";
                                                 JSONObject zonaConsegna = (JSONObject) parser.parse(jsonValue);
                                                 String id = null;
                                                 Object tecniciResponse = null;
                                                 if (zonaConsegna.containsKey("tecnicoId") && zonaConsegna.get("tecnicoId") != null) {
                                                     id = zonaConsegna.get("tecnicoId").toString();
                                                     tecniciResponse = doGetJSONObject(restAPIKey, endpoint+"/" + id, objectKey);
                                                 }

                                                 if (tecniciResponse != null) {
                                                     JSONObject tecniciObject = (JSONObject) parser.parse(tecniciResponse.toString());
                                                     JSONObject indirizzoObject = (JSONObject) tecniciObject.get("indirizzo");
                                                     Map<String, Object> recordMapTecnici = new HashMap<>();
                                                     Map<String, Object> recordFieldTecnici = new HashMap<>();
                                                     StringBuilder conditionTecnici, queryMapTecnici;
                                                     String mapNameTecnici = "tecnicoId2Technicians";
                                                     String mapModuleTecnici = "cbMap";
                                                     conditionTecnici = new StringBuilder("mapname").append("='").append(mapNameTecnici).append("'");
                                                     queryMapTecnici = new StringBuilder("select * from ").append(mapModuleTecnici).append(" where ").append(conditionTecnici);
                                                     JSONArray mapdataTecnici = wsClient.doQuery(queryMapTecnici.toString(), 3);
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
                                                     recordMapTecnici.put("searchOn", "techniciansrcid");
                                                     StringBuilder builderRemoveIndexZero = new StringBuilder(recordFieldTecnici.keySet().toString());
                                                     builderRemoveIndexZero.deleteCharAt(0);
                                                     StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                                     builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                                     String updatedfields = builderRemoveIndexLast.toString();
                                                     recordMapTecnici.put("updatedfields", updatedfields);
                                                     Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapTecnici, "POST", 3);
                                                     JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                     if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                                                         recordField.put("linktotechnician", obj.get("id").toString());
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
                             recordMap.put("elementType", moduleFieldInfo.get(fieldname));
                             recordMap.put("element", Util.getJson(recordField));
                             recordMap.put("searchOn", fieldToSearch.get(orgfieldName));
                             StringBuilder builderRemoveIndexZero = new StringBuilder(recordField.keySet().toString());
                             builderRemoveIndexZero.deleteCharAt(0);
                             StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                             builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                             String updatedfields = builderRemoveIndexLast.toString();
                             recordMap.put("updatedfields", updatedfields);
                             Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMap, "POST", 3);
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
                Map<String, String> fieldToSearch = getSearchField(parentModule);
                for (Object objRecord : recordsArray) {
                    if (objRecord instanceof JSONObject) {
                        uitype10fields = getUIType10Field(fieldname);
                        if (!uitype10fields.isEmpty()) {
                            Map<String, Object> objValue = (Map<String, Object>) objRecord;
                            String fldsearch = "";
                            if (fieldToSearch.containsKey(orgfieldName)) {
                                fldsearch = fieldToSearch.get(orgfieldName).toString();
                            }
                            Map<String, Object> recordToCreate =  getMapOfRecordToBeCreated(uitype10fields, fieldname,
                                    parentModule, objValue, fldsearch, orgfieldName);
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
                if (orgfieldName.equals("filialePartenzaId") ||
                        (orgfieldName.equals("filialeId") && !identifier.isEmpty() && identifier.equals("danni"))) {

                    if (!record.containsKey(orgfieldName) || record.get(orgfieldName) == null ) {
                        rs.put("status", "notfound");
                        rs.put("value",  "");
                        return rs;
                    }
                    Map<String, Object> searchResultCompany = searchRecord("cbCompany",
                            record.get(orgfieldName).toString(), "branchsrcid", "", false);
                    if (((boolean) searchResultCompany.get("status")) && !((boolean) searchResultCompany.get("mustbeupdated"))) {
                        rs.put("status", "found");
                        rs.put("value", searchResultCompany.get("crmid"));
                        return rs;
                    } else {
                            String endpoint = "filiali";
                            //String objectKey = "filiali";
                            String objectKey = "filiale";
                            String id = null;
                            Map<String, Object> filialiResponse = null;
                            if (record.containsKey(orgfieldName) && record.get(orgfieldName) != null) {
                                id = record.get(orgfieldName).toString();
                                endpoint = endpoint + "/" + id;
                                filialiResponse = doGetJSONObject(restAPIKey, endpoint, objectKey);
                            }

                            if (filialiResponse != null) {
                                //Map<String, Object> filialiObject = searchByID(filialiResponse, id);
                                Map<String, Object> filialiObject = filialiResponse;
                                if (!filialiObject.isEmpty()) {
                                    Map<String, Object> recordMapFiliali = new HashMap<>();
                                    Map<String, Object> recordFieldFiliali = new HashMap<>();
                                    StringBuilder conditionFiliali, queryMapFiliali;
                                    String mapNameFiliali = "filialeId2cbCompany";
                                    String mapModuleFiliali = "cbMap";
                                    conditionFiliali = new StringBuilder("mapname").append("='").append(mapNameFiliali).append("'");
                                    queryMapFiliali = new StringBuilder("select * from ").append(mapModuleFiliali).append(" where ").append(conditionFiliali);
                                    JSONArray mapdataFiliali = wsClient.doQuery(queryMapFiliali.toString(), 3);
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
                                    if (((JSONObject) parser.parse(filialiObject.toString())).get("comune") != null &&
                                            !((JSONObject) parser.parse(filialiObject.toString())).get("comune").toString().isEmpty()) {
                                        Map<String, Object> searchResultGeoboundary = searchRecord("Geoboundary",
                                                ((JSONObject) parser.parse(filialiObject.toString())).get("comune").toString(),
                                                "geoname", "", false);

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

                                    if (((JSONObject) parser.parse(filialiObject.toString())).containsKey("vettoreId") &&
                                            ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId") != null &&
                                            !((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString().isEmpty()) {
                                        searchResultVendorModule = searchRecord("Vendors",
                                                ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString(),
                                                "suppliersrcid", "Vettore", false);
                                    } else {
                                        searchResultVendorModule = new HashMap<>();
                                        searchResultVendorModule.put("status", false);
                                        searchResultVendorModule.put("crmid", "");
                                        searchResultVendorModule.put("mustbeupdated", false);
                                    }

                                    if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                        recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                    } else {
                                            String vettoriEndpoint = "vettori";
                                            String vettoriDataKey = "vettori";

                                            Object vettoriResponse = null;
                                            if (filialiObject.containsKey("vettoreId") && filialiObject.get("vettoreId") != null) {
                                                vettoriResponse = doGet(restAPIKey, vettoriEndpoint, vettoriDataKey);
                                            }

                                            if (vettoriResponse != null) {
                                                Map<String, Object> vettoriObject = searchByID(vettoriResponse,
                                                        ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString());
                                                if (!vettoriObject.isEmpty()) {
                                                    Map<String, Object> vettoriRecordMap = new HashMap<>();
                                                    Map<String, Object> vettoriRecordField = new HashMap<>();
                                                    StringBuilder vettoriCondition, vettoriQueryMap;
                                                    String vettoriMapName = "vettoreId2Vendors";
                                                    String vettoriMapModule = "cbMap";
                                                    vettoriCondition = new StringBuilder("mapname").append("='").append(vettoriMapName).append("'");
                                                    vettoriQueryMap = new StringBuilder("select * from ").append(vettoriMapModule).append(" where ").append(vettoriCondition);
                                                    JSONArray vettoriMapData = wsClient.doQuery(vettoriQueryMap.toString(), 3);
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
                                                    Object newRecord = wsClient.doInvoke(Util.methodUPSERT, vettoriRecordMap, "POST", 3);
                                                    JSONObject vettoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                    if (vettoriobjrec.containsKey("id") && !vettoriobjrec.get("id").toString().equals("")) {
                                                        recordFieldFiliali.put("linktocarrier", vettoriobjrec.get("id").toString());
                                                    }
                                                }
                                            }
                                    }

                                    /*
                                     * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                                     * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                                     * fornitoreId
                                     * */

                                    if (((JSONObject) parser.parse(filialiObject.toString())).containsKey("fornitoreId") &&
                                            ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId") != null &&
                                            !((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString().isEmpty()) {
                                        searchResultVendorModule = searchRecord("Vendors",
                                                ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString(),
                                                "suppliersrcid", "Fornitore", false);
                                    } else {
                                        searchResultVendorModule = new HashMap<>();
                                        searchResultVendorModule.put("status", false);
                                        searchResultVendorModule.put("crmid", "");
                                        searchResultVendorModule.put("mustbeupdated", false);
                                    }

                                    if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                        recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                    } else {
                                            String fornitoriEndpoint = "fornitori";
                                            //String fornitoriDataKey = "fornitori";
                                            String fornitoriDataKey = "fornitore";
                                            Map<String, Object> fornitoriResponse = null;

                                            if (filialiObject.containsKey("filialiObject") && filialiObject.get("filialiObject") != null) {
                                                fornitoriEndpoint = fornitoriEndpoint + "/" + ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString();
                                                fornitoriResponse = doGetJSONObject(restAPIKey, fornitoriEndpoint, fornitoriDataKey);

                                            }

                                            if (fornitoriResponse != null) {
                                                //Map<String, Object> fornitoriObject = searchByID(fornitoriResponse, ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString());
                                                Map<String, Object> fornitoriObject = fornitoriResponse;
                                                if (!fornitoriObject.isEmpty()) {
                                                    Map<String, Object> fornitoriRecordMap = new HashMap<>();
                                                    Map<String, Object> fornitoriRecordField = new HashMap<>();
                                                    StringBuilder fornitoriCondition, fornitoriQueryMap;
                                                    String fornitoriMapName = "fornitoreId2Vendors";
                                                    String fornitoriMapModule = "cbMap";
                                                    fornitoriCondition = new StringBuilder("mapname").append("='").append(fornitoriMapName).append("'");
                                                    fornitoriQueryMap = new StringBuilder("select * from ").append(fornitoriMapModule).append(" where ").append(fornitoriCondition);
                                                    JSONArray fornitoriMapData = wsClient.doQuery(fornitoriQueryMap.toString(), 3);
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
                                                    Object newRecord = wsClient.doInvoke(Util.methodUPSERT, fornitoriRecordMap, "POST", 3);
                                                    JSONObject fornitoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                    if (fornitoriobjrec.containsKey("id") && !fornitoriobjrec.get("id").toString().equals("")) {
                                                        recordFieldFiliali.put("vendorid", fornitoriobjrec.get("id").toString());
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
                                    Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapFiliali, "POST", 3);
                                    JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                                    if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                                        rs.put("status", "found");
                                        rs.put("value", obj.get("id").toString());
                                    }
                                }
                            }
                    }
                } else if (orgfieldName.equals("trattaId") &&
                        !identifier.isEmpty() && identifier.equals("viaggi")) {
                    Map<String, Object> searchResultServiceContracts;
                    if (record.get(orgfieldName) != null && record.get(orgfieldName).toString() != "") {
                        searchResultServiceContracts = searchRecord("ServiceContracts", record.get(orgfieldName).toString(),
                                "trattaid", "", false);
                    } else {
                        searchResultServiceContracts = new HashMap<>();
                        searchResultServiceContracts.put("status", false);
                        searchResultServiceContracts.put("crmid", "");
                        searchResultServiceContracts.put("mustbeupdated", false);
                    }

                    if (((boolean) searchResultServiceContracts.get("status")) && !((boolean) searchResultServiceContracts.get("mustbeupdated"))) {
                        rs.put("status", "found");
                        rs.put("value", searchResultServiceContracts.get("crmid"));
                    } else {
                        String tratteEndpoint = "tratte";
                        String tratteDataKey = "tratta";

                        Map<String, Object> tratteResponse = null;
                        if(record.containsKey("trattaId") && record.get("trattaId") != null) {
                            tratteEndpoint = tratteEndpoint + "/" + record.get("trattaId").toString();
                            tratteResponse = doGetJSONObject(restAPIKey, tratteEndpoint, tratteDataKey);
                        }

                        if (tratteResponse != null) {
                            Map<String, Object> tratteObject = tratteResponse;
                            if (!tratteObject.isEmpty()) {
                                Map<String, Object> tratteRecordMap = new HashMap<>();
                                Map<String, Object> tratteRecordField = new HashMap<>();
                                StringBuilder tratteCondition, tratteQueryMap;
                                String tratteMapName = "tratte2ServiceContracts";
                                String tratteMapModule = "cbMap";
                                tratteCondition = new StringBuilder("mapname").append("='").append(tratteMapName).append("'");
                                tratteQueryMap = new StringBuilder("select * from ").append(tratteMapModule).append(" where ").append(tratteCondition);
                                JSONArray tratteMapData = wsClient.doQuery(tratteQueryMap.toString(), 3);
                                JSONObject tratteQueryResult = (JSONObject)parser.parse(tratteMapData.get(0).toString());
                                JSONObject tratteMapContentJSON = (JSONObject)parser.parse(tratteQueryResult.get("contentjson").toString());
                                JSONObject tratteMapFields = (JSONObject)parser.parse(tratteMapContentJSON.get("fields").toString());
                                JSONArray tratteFieldsArray = (JSONArray) tratteMapFields.get("field");
                                for (Object field: tratteFieldsArray) {
                                    JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                    JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                    tratteRecordField.put(((JSONObject)field).get("fieldname").toString(),
                                            tratteObject.get(originalFiled.get("OrgfieldName").toString()));
                                }

                                tratteRecordField.put("assigned_user_id", wsClient.getUserID());
                                tratteRecordMap.put("elementType", "ServiceContracts");
                                tratteRecordMap.put("element", Util.getJson(tratteRecordField));
                                tratteRecordMap.put("searchOn", "trattaid");
                                StringBuilder builderRemoveIndexZero = new StringBuilder(tratteRecordField.keySet().toString());
                                builderRemoveIndexZero.deleteCharAt(0);
                                StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                String updatedfields = builderRemoveIndexLast.toString();
                                tratteRecordMap.put("updatedfields", updatedfields);
                                Object newRecord = wsClient.doInvoke(Util.methodUPSERT, tratteRecordMap, "POST", 3);
                                JSONObject tratteobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                if (tratteobjrec.containsKey("id") && !tratteobjrec.get("id").toString().equals("")) {
                                    rs.put("status", "found");
                                    rs.put("value", tratteobjrec.get("id").toString());
                                }
                            }
                        }
                    }


                } else if (orgfieldName.equals("fornitoreId") &&
                        !identifier.isEmpty() && identifier.equals("viaggi")) {
                    Map<String, Object> searchResultVendorModule;
                    if (record.get(orgfieldName) != null && record.get(orgfieldName).toString() != "") {
                        searchResultVendorModule = searchRecord("Vendors", record.get(orgfieldName).toString(),
                                "suppliersrcid", "Fornitore", false);
                    } else {
                        searchResultVendorModule = new HashMap<>();
                        searchResultVendorModule.put("status", false);
                        searchResultVendorModule.put("crmid", "");
                        searchResultVendorModule.put("mustbeupdated", false);
                    }

                    if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                        rs.put("status", "found");
                        rs.put("value", searchResultVendorModule.get("crmid"));
                    } else {
                        String fornitoriEndpoint = "fornitori";
                        String fornitoriDataKey = "fornitore";

                        Map<String, Object> fornitoriResponse = null;
                        if(record.containsKey("fornitoreId") && record.get("fornitoreId") != null) {
                            fornitoriEndpoint = fornitoriEndpoint + "/" + record.get("fornitoreId").toString();
                            fornitoriResponse = doGetJSONObject(restAPIKey, fornitoriEndpoint, fornitoriDataKey);
                        }

                        if (fornitoriResponse != null) {
                            Map<String, Object> fornitoriObject = fornitoriResponse;
                            if (!fornitoriObject.isEmpty()) {
                                Map<String, Object> fornitoriRecordMap = new HashMap<>();
                                Map<String, Object> fornitoriRecordField = new HashMap<>();
                                StringBuilder fornitoriCondition, fornitoriQueryMap;
                                String fornitoriMapName = "fornitoreId2Vendors";
                                String fornitoriMapModule = "cbMap";
                                fornitoriCondition = new StringBuilder("mapname").append("='").append(fornitoriMapName).append("'");
                                fornitoriQueryMap = new StringBuilder("select * from ").append(fornitoriMapModule).append(" where ").append(fornitoriCondition);
                                JSONArray fornitoriMapData = wsClient.doQuery(fornitoriQueryMap.toString(), 3);
                                JSONObject fornitoriQueryResult = (JSONObject)parser.parse(fornitoriMapData.get(0).toString());
                                JSONObject fornitoriMapContentJSON = (JSONObject)parser.parse(fornitoriQueryResult.get("contentjson").toString());
                                JSONObject fornitoriMapFields = (JSONObject)parser.parse(fornitoriMapContentJSON.get("fields").toString());
                                JSONArray fornitoriFieldsArray = (JSONArray) fornitoriMapFields.get("field");
                                for (Object field: fornitoriFieldsArray) {
                                    JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                    JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                    fornitoriRecordField.put(((JSONObject)field).get("fieldname").toString(),
                                            fornitoriObject.get(originalFiled.get("OrgfieldName").toString()));
                                }

                                fornitoriRecordField.put("assigned_user_id", wsClient.getUserID());
                                fornitoriRecordField.put("type", "Fornitore");
                                fornitoriRecordMap.put("elementType", "Vendors");
                                fornitoriRecordMap.put("element", Util.getJson(fornitoriRecordField));
                                fornitoriRecordMap.put("searchOn", "suppliersrcid");
                                StringBuilder builderRemoveIndexZero = new StringBuilder(fornitoriRecordField.keySet().toString());
                                builderRemoveIndexZero.deleteCharAt(0);
                                StringBuilder builderRemoveIndexLast = new StringBuilder(builderRemoveIndexZero.toString());
                                builderRemoveIndexLast.deleteCharAt(builderRemoveIndexZero.toString().length() - 1);
                                String updatedfields = builderRemoveIndexLast.toString();
                                fornitoriRecordMap.put("updatedfields", updatedfields);
                                Object newRecord = wsClient.doInvoke(Util.methodUPSERT, fornitoriRecordMap, "POST", 3);
                                JSONObject fornitoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                if (fornitoriobjrec.containsKey("id") && !fornitoriobjrec.get("id").toString().equals("")) {
                                    rs.put("status", "found");
                                    rs.put("value", fornitoriobjrec.get("id").toString());
                                }
                            }
                        }
                    }

                } else if (orgfieldName.equals("spedizioneId") &&
                        !identifier.isEmpty() && identifier.equals("danni")) {
                    Map<String, Object> searchResultShipmentModule;
                    if (record.get(orgfieldName) != null && record.get(orgfieldName).toString() != "") {
                        searchResultShipmentModule = searchRecord("Shipments", record.get(orgfieldName).toString(),
                                "pckslip_code", "", false);
                    } else {
                        searchResultShipmentModule = new HashMap<>();
                        searchResultShipmentModule.put("status", false);
                        searchResultShipmentModule.put("crmid", "");
                        searchResultShipmentModule.put("mustbeupdated", false);
                    }

                    if (((boolean) searchResultShipmentModule.get("status")) && !((boolean) searchResultShipmentModule.get("mustbeupdated"))) {
                        rs.put("status", "found");
                        rs.put("value", searchResultShipmentModule.get("crmid"));
                    } else {
                        rs.put("status", "notfound");
                        rs.put("value", "");
                    }

                } else {
                    rs.put("status", "found");
                    if (moduleDateFields.containsKey(fieldname) && (moduleDateFields.get(fieldname).equals("5") || moduleDateFields.get(fieldname).equals("50"))) {
                        String dateValue = record.get(orgfieldName).toString().replace("T", " ");
                        rs.put("value",  dateValue);
                    } else {
                        if (fieldname.toString().equals("entrusting_date") || fieldname.toString().equals("bookingdatetime")|| fieldname.toString().equals("chosenappointstart")
                                || fieldname.toString().equals("chosenappointfinish")|| fieldname.toString().equals("scheduledstart")|| fieldname.toString().equals("scheduledfinish")) {

                            if (record.get(orgfieldName) != null) {
                                String dateValue =  record.get(orgfieldName).toString().replace("T", " ");
                                rs.put("value", dateValue);
                            } else {
                                rs.put("value",  record.get(orgfieldName));
                            }
                        } else {
                            if (record.containsKey(orgfieldName)) {
                                rs.put("value",  record.get(orgfieldName));
                            } else {
                                rs.put("status", "notfound");
                                rs.put("value",  "");
                            }
                        }
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
                    String jsonValue= Util.getJson(entry.getValue());
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

    private Map<String, String> getSearchField(String parentModule) throws ParseException {
        Map<String, String> fieldmap = new HashMap<>();
        JSONParser parser = new JSONParser();
        StringBuilder mapName, condition, queryMap;
        mapName = new StringBuilder("RESTSEARCH2").append(parentModule);
        String objectModule = "cbMap";
        condition = new StringBuilder("mapname").append("='").append(mapName).append("'");
        queryMap = new StringBuilder("select *from ").append(objectModule).append(" where ").append(condition);
        JSONArray mapdata = wsClient.doQuery(queryMap.toString(), 3);
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

        if (mustBeUpdated) {
            result.put("status", false);
            result.put("crmid", "");
            result.put("mustbeupdated", mustBeUpdated);
        } else {
            if (value.contains("'")) {
                int specialCharPosition = value.indexOf("'") + 1;
                StringBuffer stringBuffer= new StringBuffer(value);
                value = stringBuffer.insert(specialCharPosition, "'").toString();
            }

            StringBuilder condition;
            if (module.equals("Vendors")) {
                if  (otherCondition.isEmpty()) {
                    condition = new StringBuilder(fieldname).append("='").append(value).append("'").append("AND type ='Fornitore'");
                } else {
                    condition = new StringBuilder(fieldname).append("='").append(value).append("'").append("AND type ='").append(otherCondition).append("'");
                }

            } else if (module.equals("cbEmployee")) {
                condition = new StringBuilder(fieldname).append("'").append(value).append("'").append("AND emptype ='").append(otherCondition).append("'");
            } else if (module.equals("ProcessLog")) {
                condition = new StringBuilder(otherCondition);
            } else {
                condition = new StringBuilder(fieldname).append("='").append(value).append("'");
            }
            StringBuilder queryString = new StringBuilder("select * from ").append(module).append(" where ").append(condition);
            JSONArray queryFormWebserviceResult = wsClient.doQuery(queryString.toString(), 3);
            if (queryFormWebserviceResult == null ||  queryFormWebserviceResult.size() == 0) {
                result.put("status", false);
                result.put("crmid", "");
                result.put("mustbeupdated", mustBeUpdated);
            } else {
                JSONParser parser = new JSONParser();
                JSONObject queryResult = (JSONObject)parser.parse(queryFormWebserviceResult.get(0).toString());
                String crmid = queryResult.get("id").toString();
                if (!crmid.isEmpty()) {
                    result.put("status", true);
                } else {
                    result.put("status", false);
                }
                result.put("crmid", crmid);
                result.put("mustbeupdated", mustBeUpdated);
            }
        }
        return result;
    }

    private Map<String, String> getUIType10Field(String module) {
        // Get All uitype 10 Module fields
        JSONObject module_info = wsClient.doDescribe(module, 3);
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
        JSONObject module_info = wsClient.doDescribe(module, 3);
        JSONArray reference_fields = (JSONArray) module_info.get("fields");
        for (Object o : reference_fields) {
            JSONObject fieldInfo = (JSONObject) o;
            if (fieldInfo.containsKey("uitype") && (Objects.equals(
                    fieldInfo.get("uitype").toString(), "50") || Objects.equals(
                            fieldInfo.get("uitype").toString(), "5"))) {
                this.moduleDateFields.put(fieldInfo.get("name").toString(), fieldInfo.get("uitype").toString());
            }
        }
    }

    private Map<String, Object> getMapOfRecordToBeCreated(Map<String, String> moduleFieldInfo, String fieldname,
                                                          String parentModule, Map element, String fieldToSearch,
                                                          String orgfieldName) throws Exception {
        Map<String, Object> recordMap = new HashMap<>();
        Map<String, Object> recordField = new HashMap<>();
        JSONParser parser = new JSONParser();
        // Get Map for Adding that Module from Rest API
        StringBuilder mapName, condition, queryMap;
        mapName = new StringBuilder(orgfieldName).append("2").append(fieldname);
        String mapModule = "cbMap";
        condition = new StringBuilder("mapname").append("='").append(mapName).append("'");
        queryMap = new StringBuilder("select * from ").append(mapModule).append(" where ").append(condition);
        JSONArray mapdata = wsClient.doQuery(queryMap.toString(), 3);
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
                    if (((JSONObject) field).get("fieldname").toString().equals("entrusting_date") || ((JSONObject) field).get("fieldname").toString().equals("bookingdatetime") ||
                            ((JSONObject) field).get("fieldname").toString().equals("chosenappointstart") || ((JSONObject) field).get("fieldname").toString().equals("chosenappointfinish") ||
                            ((JSONObject) field).get("fieldname").toString().equals("scheduledstart") || ((JSONObject) field).get("fieldname").toString().equals("scheduledfinish")) {

                        if (element.get(originalFiled.get("OrgfieldName")) != null) {
                            String dateValue = element.get(originalFiled.get("OrgfieldName")).toString().replace("T", " ");
                            recordField.put(((JSONObject)field).get("fieldname").toString(), dateValue);
                        } else {
                            recordField.put(((JSONObject)field).get("fieldname").toString(), element.get(originalFiled.get("OrgfieldName")));
                        }
                    } else {
                        recordField.put(((JSONObject)field).get("fieldname").toString(), element.get(originalFiled.get("OrgfieldName").toString()));

                    }
                }
            }
        }

        if (orgfieldName.equals("prenotazioni")) {
            /*
             * Query cbCompany module in order to check whether there already exists a record where branchsrcid == filialeId.
             */
            JSONObject prenotazioni = (JSONObject) parser.parse(element.toString());
            if (prenotazioni.containsKey("restFiliale") && prenotazioni.get("restFiliale") instanceof JSONObject && prenotazioni.get("restFiliale") != null) {
                JSONObject restFiliale = (JSONObject) prenotazioni.get("restFiliale");

                Map<String, Object> searchResultCompany;
                if (restFiliale.get("restFiliale") != null && restFiliale.containsKey("ID")) {
                    searchResultCompany = searchRecord("cbCompany",
                            restFiliale.get("ID").toString(), "branchsrcid", "", false);
                } else {
                    searchResultCompany = new HashMap<>();
                    searchResultCompany.put("status", false);
                    searchResultCompany.put("crmid", "");
                    searchResultCompany.put("mustbeupdated", false);
                }

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
                        if (!filialiObject.isEmpty()) {
                            Map<String, Object> recordMapFiliali = new HashMap<>();
                            Map<String, Object> recordFieldFiliali = new HashMap<>();
                            StringBuilder conditionFiliali, queryMapFiliali;
                            String mapNameFiliali = "filialeId2cbCompany";
                            String mapModuleFiliali = "cbMap";
                            conditionFiliali = new StringBuilder("mapname").append("='").append(mapNameFiliali).append("'");
                            queryMapFiliali = new StringBuilder("select * from ").append(mapModuleFiliali).append(" where ").append(conditionFiliali);
                            JSONArray mapdataFiliali = wsClient.doQuery(queryMapFiliali.toString(), 3);
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
                            Map<String, Object> searchResultGeoboundary;
                            if (((JSONObject) parser.parse(filialiObject.toString())).containsKey("comune") &&
                                    ((JSONObject) parser.parse(filialiObject.toString())).get("comune") != null &&
                                    !((JSONObject) parser.parse(filialiObject.toString())).get("comune").toString().isEmpty()) {
                                searchResultGeoboundary = searchRecord("Geoboundary",
                                        ((JSONObject) parser.parse(filialiObject.toString())).get("comune").toString(),
                                        "geoname", "", false);
                            } else {
                                searchResultGeoboundary = new HashMap<>();
                                searchResultGeoboundary.put("status", false);
                                searchResultGeoboundary.put("crmid", "");
                                searchResultGeoboundary.put("mustbeupdated", false);
                            }

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
                            if(((JSONObject) parser.parse(filialiObject.toString())).containsKey("vettoreId") &&
                                    ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId") != null &&
                                    !((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString().isEmpty()) {
                                searchResultVendorModule = searchRecord("Vendors",
                                        ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString(),
                                        "suppliersrcid", "Vettore", false);
                            } else {
                                searchResultVendorModule = new HashMap<>();
                                searchResultVendorModule.put("status", false);
                                searchResultVendorModule.put("crmid", "");
                                searchResultVendorModule.put("mustbeupdated", false);
                            }

                            if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                            } else {
                                    String vettoriEndpoint = "vettori";
                                    String vettoriDataKey = "vettori";
                                    Object vettoriResponse = null;
                                    if (filialiObject.containsKey("vettoreId") && filialiObject.get("vettoreId") != null) {
                                        vettoriResponse = doGet(restAPIKey, vettoriEndpoint, vettoriDataKey);
                                    }

                                    if (vettoriResponse != null) {
                                        Map<String, Object> vettoriObject = searchByID(vettoriResponse,
                                                ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString());
                                        if (!vettoriObject.isEmpty()) {
                                            Map<String, Object> vettoriRecordMap = new HashMap<>();
                                            Map<String, Object> vettoriRecordField = new HashMap<>();
                                            StringBuilder vettoriCondition, vettoriQueryMap;
                                            String vettoriMapName = "vettoreId2Vendors";
                                            String vettoriMapModule = "cbMap";
                                            vettoriCondition = new StringBuilder("mapname").append("='").append(vettoriMapName).append("'");
                                            vettoriQueryMap = new StringBuilder("select * from ").append(vettoriMapModule).append(" where ").append(vettoriCondition);
                                            JSONArray vettoriMapData = wsClient.doQuery(vettoriQueryMap.toString(), 3);
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
                                            Object newRecord = wsClient.doInvoke(Util.methodUPSERT, vettoriRecordMap, "POST", 3);
                                            JSONObject vettoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                            if (vettoriobjrec.containsKey("id") && !vettoriobjrec.get("id").toString().equals("")) {
                                                recordFieldFiliali.put("linktocarrier", vettoriobjrec.get("id").toString());
                                            }
                                        }
                                    }
                            }

                            /*
                             * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                             * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                             * fornitoreId
                             * */
                            if (((JSONObject) parser.parse(filialiObject.toString())).containsKey("fornitoreId") &&
                                    ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId") != null &&
                                    !((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString().isEmpty()) {
                                searchResultVendorModule = searchRecord("Vendors",
                                        ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString(),
                                        "suppliersrcid", "Fornitore", false);
                            } else {
                                searchResultVendorModule = new HashMap<>();
                                searchResultVendorModule.put("status", false);
                                searchResultVendorModule.put("crmid", "");
                                searchResultVendorModule.put("mustbeupdated", false);
                            }
                            if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                            } else {
                                    String fornitoriEndpoint = "fornitori";
                                    //String fornitoriDataKey = "fornitori";
                                    String fornitoriDataKey = "fornitore";

                                    Map<String, Object> fornitoriResponse = null;
                                    if(filialiObject.containsKey("fornitoreId") && filialiObject.get("fornitoreId") != null) {
                                        fornitoriEndpoint = fornitoriEndpoint + "/" + ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString();
                                        fornitoriResponse = doGetJSONObject(restAPIKey, fornitoriEndpoint, fornitoriDataKey);
                                    }

                                    if (fornitoriResponse != null) {
                                        //Map<String, Object> fornitoriObject = searchByID(fornitoriResponse, ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString());
                                        Map<String, Object> fornitoriObject = fornitoriResponse;
                                        if (!fornitoriObject.isEmpty()) {
                                            Map<String, Object> fornitoriRecordMap = new HashMap<>();
                                            Map<String, Object> fornitoriRecordField = new HashMap<>();
                                            StringBuilder fornitoriCondition, fornitoriQueryMap;
                                            String fornitoriMapName = "fornitoreId2Vendors";
                                            String fornitoriMapModule = "cbMap";
                                            fornitoriCondition = new StringBuilder("mapname").append("='").append(fornitoriMapName).append("'");
                                            fornitoriQueryMap = new StringBuilder("select * from ").append(fornitoriMapModule).append(" where ").append(fornitoriCondition);
                                            JSONArray fornitoriMapData = wsClient.doQuery(fornitoriQueryMap.toString(), 3);
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
                                            Object newRecord = wsClient.doInvoke(Util.methodUPSERT, fornitoriRecordMap, "POST", 3);
                                            JSONObject fornitoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                            if (fornitoriobjrec.containsKey("id") && !fornitoriobjrec.get("id").toString().equals("")) {
                                                recordFieldFiliali.put("vendorid", fornitoriobjrec.get("id").toString());
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
                            Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapFiliali, "POST", 3);
                            JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                            if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                                recordField.put("linktobranch", obj.get("id").toString());
                            }
                        }
                    }
                }
            }


            if (prenotazioni.containsKey("restAutista") && prenotazioni.get("restAutista") instanceof JSONObject && prenotazioni.get("restAutista") != null) {
                /*
                 * Query cbEmployee module in order to check whether there already exists a record where nif == restAutista.ID AND emptype == 'Autista'.
                 * If there exists none, then create a new one with the following mapping:
                 * */
                JSONObject restAutista = (JSONObject) prenotazioni.get("restAutista");
                Map<String, Object> searchResultEmployee;
                if (restAutista.containsKey("ID") && restAutista.get("ID") != null && !restAutista.get("ID").toString().isEmpty()) {
                    searchResultEmployee = searchRecord("cbEmployee",
                            restAutista.get("ID").toString(), "nif", "Autista", true);
                } else {
                    searchResultEmployee = new HashMap<>();
                    searchResultEmployee.put("status", false);
                    searchResultEmployee.put("crmid", "");
                    searchResultEmployee.put("mustbeupdated", false);
                }


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
                    StringBuilder conditionRestAutista, queryMapRestAutista;
                    String mapNameRestAutista = "restAutista2cbEmployee";
                    String mapModuleRestAutista = "cbMap";
                    conditionRestAutista = new StringBuilder("mapname").append("='").append(mapNameRestAutista).append("'");
                    queryMapRestAutista = new StringBuilder("select * from ").append(mapModuleRestAutista).append(" where ").append(conditionRestAutista);
                    JSONArray mapdataRestAutista = wsClient.doQuery(queryMapRestAutista.toString(), 3);
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
                        if (parser.parse(restAutista.toString()) != null) {
                            String endpoint = "filiali";
                            //String objectKey = "filiali";
                            String objectKey = "filiale";

                            String id = null;
                            Map<String, Object> filialiResponse = null;
                            if(restAutista.containsKey("filialeId") && restAutista.get("filialeId") != null) {
                                id = restAutista.get("filialeId").toString();
                                endpoint = endpoint + "/" + id;
                                filialiResponse = doGetJSONObject(restAPIKey, endpoint, objectKey);
                            }

                            if (filialiResponse != null) {
                                //Map<String, Object> filialiObject = searchByID(filialiResponse, id);
                                Map<String, Object> filialiObject = filialiResponse;
                                if (!filialiObject.isEmpty()) {
                                    Map<String, Object> recordMapFiliali = new HashMap<>();
                                    Map<String, Object> recordFieldFiliali = new HashMap<>();
                                    StringBuilder conditionFiliali, queryMapFiliali;
                                    String mapNameFiliali = "filialeId2cbCompany";
                                    String mapModuleFiliali = "cbMap";
                                    conditionFiliali = new StringBuilder("mapname").append("='").append(mapNameFiliali).append("'");
                                    queryMapFiliali = new StringBuilder("select * from ").append(mapModuleFiliali).append(" where ").append(conditionFiliali);
                                    JSONArray mapdataFiliali = wsClient.doQuery(queryMapFiliali.toString(), 3);
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
                                    Map<String, Object> searchResultGeoboundary;
                                    if (((JSONObject) parser.parse(filialiObject.toString())).containsKey("comune") &&
                                            ((JSONObject) parser.parse(filialiObject.toString())).get("comune") != null &&
                                            !((JSONObject) parser.parse(filialiObject.toString())).get("comune").toString().isEmpty()) {
                                        searchResultGeoboundary = searchRecord("Geoboundary",
                                                ((JSONObject) parser.parse(filialiObject.toString())).get("comune").toString(),
                                                "geoname", "", false);
                                    } else {
                                        searchResultGeoboundary = new HashMap<>();
                                        searchResultGeoboundary.put("status", false);
                                        searchResultGeoboundary.put("crmid", "");
                                        searchResultGeoboundary.put("mustbeupdated", false);
                                    }

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
                                    if(((JSONObject) parser.parse(filialiObject.toString())).containsKey("vettoreId") &&
                                            ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId") != null &&
                                            !((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString().isEmpty()) {
                                        searchResultVendorModule = searchRecord("Vendors",
                                                ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString(),
                                                "suppliersrcid", "Vettore", false);
                                    } else {
                                        searchResultVendorModule = new HashMap<>();
                                        searchResultVendorModule.put("status", false);
                                        searchResultVendorModule.put("crmid", "");
                                        searchResultVendorModule.put("mustbeupdated", false);
                                    }

                                    if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                        recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                    } else {
                                            String vettoriEndpoint = "vettori";
                                            String vettoriDataKey = "vettori";

                                            Object vettoriResponse = null;
                                            if (filialiObject.containsKey("vettoreId") && filialiObject.get("vettoreId") != null) {
                                                vettoriResponse = doGet(restAPIKey, vettoriEndpoint, vettoriDataKey);
                                            }

                                            if (vettoriResponse != null) {
                                                Map<String, Object> vettoriObject = searchByID(vettoriResponse,
                                                        ((JSONObject) parser.parse(filialiObject.toString())).get("vettoreId").toString());
                                                if (!vettoriObject.isEmpty()) {
                                                    Map<String, Object> vettoriRecordMap = new HashMap<>();
                                                    Map<String, Object> vettoriRecordField = new HashMap<>();
                                                    StringBuilder vettoriCondition, vettoriQueryMap;
                                                    String vettoriMapName = "vettoreId2Vendors";
                                                    String vettoriMapModule = "cbMap";
                                                    vettoriCondition = new StringBuilder("mapname").append("='").append(vettoriMapName).append("'");
                                                    vettoriQueryMap = new StringBuilder("select * from ").append(vettoriMapModule).append(" where ").append(vettoriCondition);
                                                    JSONArray vettoriMapData = wsClient.doQuery(vettoriQueryMap.toString(), 3);
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
                                                    Object newRecord = wsClient.doInvoke(Util.methodUPSERT, vettoriRecordMap, "POST", 3);
                                                    JSONObject vettoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                    if (vettoriobjrec.containsKey("id") && !vettoriobjrec.get("id").toString().equals("")) {
                                                        recordFieldFiliali.put("linktocarrier", vettoriobjrec.get("id").toString());
                                                    }
                                                }
                                            }
                                    }

                                    /*
                                     * Query Vendors module in order to check whether there already exists a record where suppliersrcid == vettoreId AND type == 'Vettore'.
                                     * If there exists none, then call the api/vettori endpoint and retrieve the object where ID==vettoreId. Then, create a new Vendor in CoreBOS
                                     * fornitoreId
                                     * */
                                    if (((JSONObject) parser.parse(filialiObject.toString())).containsKey("fornitoreId") &&
                                            ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId") != null &&
                                            !((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString().isEmpty()) {
                                        searchResultVendorModule = searchRecord("Vendors",
                                                ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString(),
                                                "suppliersrcid", "Fornitore", false);
                                    } else {
                                        searchResultVendorModule = new HashMap<>();
                                        searchResultVendorModule.put("status", false);
                                        searchResultVendorModule.put("crmid", "");
                                        searchResultVendorModule.put("mustbeupdated", false);
                                    }

                                    if (((boolean) searchResultVendorModule.get("status")) && !((boolean) searchResultVendorModule.get("mustbeupdated"))) {
                                        recordFieldFiliali.put("linktocarrier", searchResultVendorModule.get("crmid"));
                                    } else {
                                            String fornitoriEndpoint = "fornitori";
                                            //String fornitoriDataKey = "fornitori";
                                            String fornitoriDataKey = "fornitore";

                                            Map<String, Object> fornitoriResponse = null;
                                            if(filialiObject.containsKey("fornitoreId") && filialiObject.get("fornitoreId") != null) {
                                                fornitoriEndpoint = fornitoriEndpoint + "/" + ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString();
                                                fornitoriResponse = doGetJSONObject(restAPIKey, fornitoriEndpoint, fornitoriDataKey);
                                            }

                                            if (fornitoriResponse != null) {
                                                //Map<String, Object> fornitoriObject = searchByID(fornitoriResponse, ((JSONObject) parser.parse(filialiObject.toString())).get("fornitoreId").toString());
                                                Map<String, Object> fornitoriObject = fornitoriResponse;
                                                if (!fornitoriObject.isEmpty()) {
                                                    Map<String, Object> fornitoriRecordMap = new HashMap<>();
                                                    Map<String, Object> fornitoriRecordField = new HashMap<>();
                                                    StringBuilder fornitoriCondition, fornitoriQueryMap;
                                                    String fornitoriMapName = "fornitoreId2Vendors";
                                                    String fornitoriMapModule = "cbMap";
                                                    fornitoriCondition = new StringBuilder("mapname").append("='").append(fornitoriMapName).append("'");
                                                    fornitoriQueryMap = new StringBuilder("select * from ").append(fornitoriMapModule).append(" where ").append(fornitoriCondition);
                                                    JSONArray fornitoriMapData = wsClient.doQuery(fornitoriQueryMap.toString(), 3);
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
                                                    Object newRecord = wsClient.doInvoke(Util.methodUPSERT, fornitoriRecordMap, "POST", 3);
                                                    JSONObject fornitoriobjrec = (JSONObject)parser.parse(Util.getJson(newRecord));
                                                    if (fornitoriobjrec.containsKey("id") && !fornitoriobjrec.get("id").toString().equals("")) {
                                                        recordFieldFiliali.put("vendorid", fornitoriobjrec.get("id").toString());
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
                                    Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapFiliali, "POST", 3);
                                    JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                                    if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                                        recordMapRestAutista.put("linktobranch", obj.get("id").toString());
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
                    Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapRestAutista, "POST", 3);
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
            JSONObject prodottiObject = (JSONObject) parser.parse(element.toString());
            if (prodottiObject.containsKey("categoryId") && prodottiObject.get("categoryId") != null) {
                Map<String, Object> searchResultCbproductcategory = searchRecord("cbproductcategory",
                        prodottiObject.get("categoryId").toString(), "categorysrcid", "", false);

                if (((boolean) searchResultCbproductcategory.get("status")) && !((boolean) searchResultCbproductcategory.get("mustbeupdated"))) {
                    Map<String, String> referenceFields = getUIType10Field(fieldname);
                    for (Object key : referenceFields.keySet()) {
                        String keyStr = (String)key;
                        if (referenceFields.get(keyStr).equals("cbproductcategory")) {
                            recordField.put(keyStr, searchResultCbproductcategory.get("crmid"));
                        }
                    }
                } else {
                        String endpoint = "categorieMerceologiche";
                        String objectKey = "categorieMerceologiche";
                        String id = null;
                        Object categorieMerceologicheResponse = null;
                        if (prodottiObject.containsKey("categoryId") && prodottiObject.get("categoryId") != null) {
                            id = prodottiObject.get("categoryId").toString();
                            categorieMerceologicheResponse = doGet(restAPIKey, endpoint, objectKey);
                        }

                        if (categorieMerceologicheResponse != null) {
                            Map<String, Object> categorieMerceologicheObject = searchByID(categorieMerceologicheResponse, id);
                            if (!categorieMerceologicheObject.isEmpty()) {
                                Map<String, Object> recordMapCategoryId = new HashMap<>();
                                Map<String, Object> recordFieldCategoryId = new HashMap<>();
                                StringBuilder conditionCategoryId, queryMapCategoryId;
                                String mapNameCategoryId = "categoryId2cbproductcategory";
                                String mapModuleCategoryId = "cbMap";
                                conditionCategoryId = new StringBuilder("mapname").append("='").append(mapNameCategoryId).append("'");
                                queryMapCategoryId = new StringBuilder("select * from ").append(mapModuleCategoryId).append(" where ").append(conditionCategoryId);
                                JSONArray mapdataCategoryId = wsClient.doQuery(queryMapCategoryId.toString(), 3);
                                JSONObject resultCategoryId = (JSONObject)parser.parse(mapdataCategoryId.get(0).toString());
                                JSONObject contentjsonCategoryId = (JSONObject)parser.parse(resultCategoryId.get("contentjson").toString());
                                JSONObject fieldsFiliali = (JSONObject)parser.parse(contentjsonCategoryId.get("fields").toString());
                                JSONArray fields_arrayFiliali = (JSONArray) fieldsFiliali.get("field");
                                for (Object field: fields_arrayFiliali) {
                                    JSONObject originalFields = (JSONObject) ((JSONObject)field).get("Orgfields");
                                    JSONObject originalFiled = (JSONObject) originalFields.get("Orgfield");
                                    recordFieldCategoryId.put(((JSONObject)field).get("fieldname").toString(), categorieMerceologicheObject.get(originalFiled.get("OrgfieldName").toString()));
                                }
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
                                Object newRecord = wsClient.doInvoke(Util.methodUPSERT, recordMapCategoryId, "POST", 3);
                                JSONObject obj = (JSONObject)parser.parse(Util.getJson(newRecord));
                                if (obj.containsKey("id") && !obj.get("id").toString().equals("")) {
                                    recordField.put("linktocategory", obj.get("id").toString());
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
        return recordMap;
    }

    private void createRecordsInMap(Map<String, String> moduleCRMID) throws ParseException {
        for (Map<String, Object> record: lastRecordToCreate
             ) {
            String module = record.get("elementType").toString();
            Map<String, String> uitype10fields = getUIType10Field(module);
            JSONParser parser = new JSONParser();
            JSONObject recordFields = (JSONObject) parser.parse(record.get("element").toString());
            for (Object key : uitype10fields.keySet()) {
                String keyStr = (String)key;
                if (moduleCRMID.containsKey(uitype10fields.get(keyStr))) {
                    // set the field value
                    recordFields.put(keyStr, moduleCRMID.get(uitype10fields.get(keyStr)));
                } else {
                    if (recordFields.containsKey(keyStr) && recordFields.get(keyStr) != null &&
                            recordFields.get(keyStr) != "") {
                        // TODO: 4/10/20 Scenario for Prodotti
                        Map<String, String> fieldToSearch = getSearchField(module);
                        if (recordFields.get(keyStr).toString().contains("x")) {
                            recordFields.put(keyStr, recordFields.get(keyStr));
                        } else {
                            Map<String, Object> searchResult = searchRecord(uitype10fields.get(keyStr),
                                    String.valueOf(recordFields.get(keyStr)), fieldToSearch.get(keyStr).toString(),
                                    "", false);
                            if (((boolean) searchResult.get("status"))) {
                                recordFields.put(keyStr, searchResult.get("crmid"));
                            }
                        }
                    }
                }
            }
            record.put("element", Util.getJson(recordFields));
            Object d = wsClient.doInvoke(Util.methodUPSERT, record, "POST", 3);
        }
    }

    private Map<String, Object> searchByID(Object response, String id) throws ParseException {
        Map<String, Object> objValue = new HashMap<>();
        if (id == null || id.isEmpty() || id.equals("null")) {
            return objValue;
        }
        JSONParser parser = new JSONParser();
        JSONArray resArray = (JSONArray) parser.parse(response.toString());
        for (Object object: resArray
             ) {
            JSONObject record = (JSONObject) parser.parse(object.toString());
            if (record.get("ID").toString().equals(id)) {
                objValue = record;
                return objValue;
            }
        }
        return objValue;
    }

    private JSONObject doGetJSONObject(String apiKey, String _endpoint, String key) {
        Map<String, String> mapToSend = new HashMap<>();
        Header[] headersArray = new Header[2];
        headersArray[0] = new BasicHeader("Content-type", "application/json");
        headersArray[1] = new BasicHeader("OPERATOR-API-KEY", apiKey);
        return restClient.doGetJSONObject(_endpoint, mapToSend, headersArray, key);
    }

    private JSONArray doGet(String apiKey, String _endpoint, String key) {
        Map<String, String> mapToSend = new HashMap<>();
        Header[] headersArray = new Header[2];
        headersArray[0] = new BasicHeader("Content-type", "application/json");
        headersArray[1] = new BasicHeader("OPERATOR-API-KEY", apiKey);
        return restClient.doGet(_endpoint, mapToSend, headersArray, key);
    }
}