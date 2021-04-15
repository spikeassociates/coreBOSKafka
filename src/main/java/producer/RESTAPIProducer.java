package producer;

import helper.Config;
import helper.Util;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import service.RESTClient;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class RESTAPIProducer {
    private final String topic = (Util.getProperty("kafka.topicname").isEmpty()) ? System.getenv("TOPIC_NAME") : Util.getProperty("kafka.topicname");
    private final String rest_api_url = (Util.getProperty("producer.fetch.url").isEmpty()) ? System.getenv("API_BASE_URL") : Util.getProperty("producer.fetch.url");
    private final String _endpoint = (Util.getProperty("producer.fetch.endpoint").isEmpty()) ? System.getenv("END_POINT") : Util.getProperty("producer.fetch.endpoint");
    public final String restAPIKey = (Util.getProperty("producer.fetch.url.apikey").isEmpty()) ? System.getenv("API_KEY") : Util.getProperty("producer.fetch.url.apikey");
    public String filterByCurrentDate = (Util.getProperty("producer.fetch.filter.currentdate").isEmpty()) ? System.getenv("FILTER_BY_CURRENT_DATE_FLAG") : Util.getProperty("producer.fetch.filter.currentdate");
    public String dateToFilterRecords = (Util.getProperty("producer.fetch.filter.date").isEmpty()) ? System.getenv("FILTER_BY_THIS_DATE") : Util.getProperty("producer.fetch.filter.date");
    protected static final String KAFKA_URL = (Util.getProperty("kafka.url").isEmpty()) ? System.getenv("KAFKA_HOST") : Util.getProperty("kafka.url");
    private final String pagesize = (Util.getProperty("producer.fetch.response.pagesize").isEmpty()) ? System.getenv("PAGE_SIZE") : Util.getProperty("producer.fetch.response.pagesize");
    private String startDateTime = (Util.getProperty("producer.fetch.filter.startdate").isEmpty()) ? System.getenv("FILTER_START_DATE") : Util.getProperty("producer.fetch.filter.startdate");
    private String endDateTime = (Util.getProperty("producer.fetch.filter.enddate").isEmpty()) ? System.getenv("FILTER_END_DATE") : Util.getProperty("producer.fetch.filter.enddate");
    private String fetchResponseKey = (Util.getProperty("producer.responsekey").isEmpty()) ? System.getenv("FETCH_RESPONSE_KEY") : Util.getProperty("producer.responsekey");
    private String corebosModuleToSync = (Util.getProperty("corebos.sync.module").isEmpty()) ? System.getenv("COREBOS_MODULE_TO_SYNC") : Util.getProperty("corebos.sync.module");
    private String identifier = (Util.getProperty("producer.identity").isEmpty()) ? System.getenv("PRODUCER_IDENTIFIER") : Util.getProperty("producer.identity");


    private String securityProtocol = (Util.getProperty("kafka.security.protocol").isEmpty()) ?
            System.getenv("KAFKA_SECURITY_PROTOCOL") : Util.getProperty("kafka.security.protocol");
    private String saslMechanism = (Util.getProperty("kafka.sasl.mechanism").isEmpty()) ?
            System.getenv("KAFKA_SASL_MECHANISM") : Util.getProperty("kafka.sasl.mechanism");
    private String sasljaasconfig =  (Util.getProperty("kafka.sasl.jaas.config").isEmpty()) ?
            System.getenv("KAFKA_SASL_JAAS_CONFIG") : Util.getProperty("kafka.sasl.jaas.config");

    protected static org.apache.kafka.clients.producer.Producer<String, String> producer;
    protected RESTClient restClient;

    public RESTAPIProducer() {
        restClient = new RESTClient(rest_api_url);
        Properties props = new Properties();
        props.put("metadata.broker.list", KAFKA_URL);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("request.required.acks", "1");
        props.put("security.protocol", securityProtocol);
        props.put("sasl.mechanism", saslMechanism);
        props.put("sasl.jaas.config", sasljaasconfig);
        producer = new KafkaProducer(props);
    }

    protected void publishMessage(String topic, String key, String message, int partition) {
        String runtime = new Date().toString();
        String msg = "Message Publishing Time - " + runtime + message;
        System.out.println(msg);
        try {
            RecordMetadata metadata = (RecordMetadata) producer.send(new ProducerRecord(topic, partition, key, message)).get();
            System.out.printf("Record sent with key %s to partition %d with offset " + metadata.offset() + " with value %s Time %s"
                     , key, metadata.partition(), message, runtime);
            System.out.println("topic = " + topic);
            System.out.println("key = " + key);
            System.out.println("message = " + message);
            System.out.println("metadata.partition() = " + metadata.partition());
            System.out.println(msg);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void init() throws ParseException {
        String isFirstRequest = Config.getInstance().isFirstRequest();
        if (isFirstRequest.isEmpty() || isFirstRequest.equals("YES")) {
            int pageSize = Integer.parseInt(Objects.requireNonNull(pagesize));
            int pageNr = 1;

            Object response = doGet(restAPIKey, pageSize, pageNr, startDateTime, endDateTime, getDateForFiltering());

            if (response == null)
                return;

            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(response.toString());

            /*
             * Check the Response from the REST API is 200
             */
//            JSONObject responseStatus = (JSONObject) jsonObject.get("status");
//            if (Integer.parseInt(responseStatus.get("code").toString()) != 200) {
//                System.out.println(responseStatus.get("code") + "::" + responseStatus.get("message"));
//                return;
//            }
            int totalNumberOfRecords = 0;
            if (!identifier.isEmpty() && identifier.equals("spedizioni")) {
                totalNumberOfRecords = Integer.parseInt(jsonObject.get("nSpedizioniAccordingFilters").toString());
            } else if (!identifier.isEmpty() && identifier.equals("viaggi")) {
                totalNumberOfRecords = Integer.parseInt(jsonObject.get("numberOfRecords").toString());
            } else if (!identifier.isEmpty() && identifier.equals("danni")) {
                totalNumberOfRecords = Integer.parseInt(jsonObject.get("numOfRecords").toString());
            }

            int numberOfPages;
            if ((totalNumberOfRecords % pageSize) == 0) {
                numberOfPages = totalNumberOfRecords / pageSize;
            } else {
                numberOfPages = totalNumberOfRecords / pageSize;
                numberOfPages = numberOfPages + 1;
            }

            if (numberOfPages != 0) {
                Config.getInstance().setTotalNumberOfPages("" + numberOfPages);
                Config.getInstance().setFirstRequest("" + "NO");
                processResponseData(response, 1);
                Config.getInstance().setCurrentPartition("1");
                if (numberOfPages == 1) {
                    Config.getInstance().setFirstRequest("" + "YES");
                    producer.close();
                } else {
                    init();
                }
            }
        } else {
            int pageSize = Integer.parseInt(Objects.requireNonNull(pagesize));
            int savedPageNumbers = Integer.parseInt(Config.getInstance().getTotalNumberOfPages());
            Config.getInstance().setFirstRequest("" + "YES");
            for (int page = 2; page <= savedPageNumbers; page++) {
                Object response = doGet(restAPIKey, pageSize, page, startDateTime, endDateTime, getDateForFiltering());
                if (Integer.parseInt(Config.getInstance().getCurrentPartition()) % 10 == 0) {
                    processResponseData(response, 1);
                    Config.getInstance().setCurrentPartition("1"); // reset partition
                } else {
                    processResponseData(response, Integer.parseInt(Config.getInstance().getCurrentPartition()) + 1);
                    Config.getInstance().setCurrentPartition(String.valueOf((Integer.parseInt(Config.getInstance().getCurrentPartition()) + 1)));
                }
            }
            producer.close();
        }
    }

    private JSONObject getShipmentStatus(Object response) throws ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(response.toString());
        return (JSONObject) jsonObject.get("mappaEsitiPerSpedizione");
    }

    private JSONArray getShipmentsData(Object response) throws ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(response.toString());
        return (JSONArray) jsonObject.get(fetchResponseKey);
    }

    private JSONArray getJSONArrayDataByKey(Object response, String key) throws ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(response.toString());
        return (JSONArray) jsonObject.get(key);
    }

    private JSONObject getJSONObjectDataDataByKey(Object response, String key) throws ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(response.toString());
        return (JSONObject) jsonObject.get(key);
    }

    private Object doGet(String apiKey, int pageSize, int pageNumber, String startDateTime, String endDateTime, String currentDateTime) {
        Map<String, String> mapToSend = new HashMap<>();
        mapToSend.put("pageSize", String.valueOf(pageSize));
        mapToSend.put("pageNr", String.valueOf(pageNumber));
        if (!filterByCurrentDate.equals("yes")) {
            if (!identifier.isEmpty() && identifier.equals("spedizioni")) {
                mapToSend.put("dataRegistrazioneEsitoDal", startDateTime);
                mapToSend.put("dataRegistrazioneEsitoAl", endDateTime);
            } else if (!identifier.isEmpty() && identifier.equals("viaggi")) {
                mapToSend.put("dataDal", startDateTime);
                mapToSend.put("dataAl", endDateTime);
            } else if (!identifier.isEmpty() && identifier.equals("danni")) {
                mapToSend.put("dataAperturaDal", startDateTime);
                mapToSend.put("dataAperturaAl", endDateTime);
            }
        } else {
            switch (identifier) {
                case "spedizioni":
                    mapToSend.put("dataRegistrazioneEsitoDal", currentDateTime);
                    mapToSend.put("dataRegistrazioneEsitoAl", currentDateTime);
                    break;
                case "viaggi":
                    mapToSend.put("dataDal", currentDateTime);
                    mapToSend.put("dataAl", currentDateTime);
                    break;
                case "danni":
                    mapToSend.put("dataAperturaDal", currentDateTime);
                    mapToSend.put("dataAperturaAl", currentDateTime);
                    break;
            }
        }

        Header[] headersArray = new Header[2];
        headersArray[0] = new BasicHeader("Content-type", "application/json");
        headersArray[1] = new BasicHeader("OPERATOR-API-KEY", apiKey);
        Object response = restClient.doGet(_endpoint, mapToSend, headersArray);
        if (response == null)
            return null;
        long currentTime = new Date().getTime() / 1000;
        Config.getInstance().setLastTimeStampToSync("" + currentTime);
        return response;
    }

    private String getDateForFiltering() {
        // DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        if (filterByCurrentDate.equals("yes")) {
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDateTime currentDate = LocalDateTime.now();
            return dateTimeFormatter.format(currentDate.minusDays(1));
        } else {
            return dateToFilterRecords;
        }
    }

    private void processResponseData(Object response, int currentPage) throws ParseException {
        System.out.println("Waiting for Result");
        if (response == null)
            return;
        if (!identifier.isEmpty() && identifier.equals("spedizioni")) {
            JSONArray shipmentsData = getShipmentsData(response);
            JSONObject shipmentsStatus = getShipmentStatus(response);

            String partitionKey = "partition-" + currentPage;
            for (Object shipment : shipmentsData) {
                /* We Combine Shipment and their Status together because Status(Process Log in CoreBos Application) are for the Shipment so
                 * this helps even if the Consumer Crush we can continue to without problem
                 * but if we separate them as we do now in case of consumer crush when we Re0run consumer may result to problem for example
                 * the partition assigned to the Consumer is of shipment statuses so those statuses it may happen that their shipments are not created
                 * * */
                JSONObject messageToSend = new JSONObject();
                messageToSend.put("operation", Util.methodUPDATE);
                messageToSend.put("shipment", shipment);

                JSONParser parser = new JSONParser();
                JSONObject currentShipment = (JSONObject) parser.parse(shipment.toString());
                String currentShipmentID = String.valueOf(currentShipment.get("ID"));
                JSONObject status = new JSONObject();
                status.put(currentShipment.get("ID").toString() , shipmentsStatus.get(currentShipmentID));
                messageToSend.put("status", status);

                int partition = currentPage - 1;
                publishMessage(topic, partitionKey, Util.getJson(messageToSend), partition);
            }
        } else if (!identifier.isEmpty() && identifier.equals("viaggi")) {
            JSONArray viaggiData = getJSONArrayDataByKey(response, fetchResponseKey);
            JSONObject mappaQuotaTrazioni = getJSONObjectDataDataByKey(response, "mappaQuotaTrazioni");
            String partitionKey = "partition-" + currentPage;
            for (Object viaggi : viaggiData) {
                JSONObject messageToSend = new JSONObject();
                messageToSend.put("operation", Util.methodUPDATE);
                JSONParser parser = new JSONParser();
                JSONObject currentViaggi = (JSONObject) parser.parse(viaggi.toString());
//                System.out.println(currentViaggi.get("ID"));
//                System.out.println(mappaQuotaTrazioni.get(currentViaggi.get("ID").toString()));
                currentViaggi.put("quotatrazione", mappaQuotaTrazioni.get(currentViaggi.get("ID").toString()));
                messageToSend.put("viaggi", currentViaggi);
                int partition = currentPage - 1;
                publishMessage(topic, partitionKey, Util.getJson(messageToSend), partition);
            }
        } else if (!identifier.isEmpty() && identifier.equals("danni")) {
            JSONArray danniData = getJSONArrayDataByKey(response, fetchResponseKey);
            String partitionKey = "partition-" + currentPage;
            for (Object danni : danniData) {
                JSONObject messageToSend = new JSONObject();
                messageToSend.put("operation", Util.methodUPDATE);
                messageToSend.put("danni", danni);
                int partition = currentPage - 1;
                publishMessage(topic, partitionKey, Util.getJson(messageToSend), partition);
            }
        }

        /*for (Object key : shipmentsStatus.keySet()) {
            String shipmentid = (String) key;
            Object status = shipmentsStatus.get(shipmentid);
            String module = "ProcessLog";
            KeyData keyData = new KeyData();
            keyData.module = module;
            keyData.operation = Util.methodUPDATE;
            JSONObject singleStatus = new JSONObject();
            singleStatus.put(shipmentid, status);
            JSONObject messageToSend = new JSONObject();
            messageToSend.put("operation", keyData);
            messageToSend.put("data", singleStatus);
            int partition = currentPage - 1;
            publishMessage(topic, partitionKey, Util.getJson(messageToSend), partition);
        }**/
        Config.getInstance().save();
    }
}
