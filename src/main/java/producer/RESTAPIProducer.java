package producer;

import helper.Config;
import helper.Util;
import model.KeyData;
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
    private final String topic = (Util.getProperty("corebos.restproducer.topic").isEmpty()) ? System.getenv("TOPIC_NAME") : Util.getProperty("corebos.restproducer.topic");
    private final String rest_api_url = (Util.getProperty("corebos.restproducer.url").isEmpty()) ? System.getenv("API_BASE_URL") : Util.getProperty("corebos.restproducer.url");
    private final String _endpoint = (Util.getProperty("corebos.restproducer.endpoint").isEmpty()) ? System.getenv("END_POINT") : Util.getProperty("corebos.restproducer.endpoint");
    public final String restAPIKey = (Util.getProperty("corebos.restproducer.restapikey").isEmpty()) ? System.getenv("API_KEY") : Util.getProperty("corebos.restproducer.restapikey");
    public String filterByCurrentDate = (Util.getProperty("corebos.restproducer.filterbycurrentdate").isEmpty()) ? System.getenv("FILTER_BY_CURRENT_DATE_FLAG") : Util.getProperty("corebos.restproducer.filterbycurrentdate");
    public String dateToFilterRecords = (Util.getProperty("corebos.restproducer.date").isEmpty()) ? System.getenv("FILTER_BY_THIS_DATE") : Util.getProperty("corebos.restproducer.date");
    protected static final String KAFKA_URL = (Util.getProperty("corebos.kafka.url").isEmpty()) ? System.getenv("KAFKA_HOST") : Util.getProperty("corebos.kafka.url");
    private final String pagesize = (Util.getProperty("corebos.restproducer.pagesize").isEmpty()) ? System.getenv("PAGE_SIZE") : Util.getProperty("corebos.restproducer.pagesize");
    private String startDateTime = (Util.getProperty("corebos.restproducer.startdate").isEmpty()) ? System.getenv("FILTER_START_DATE") : Util.getProperty("corebos.restproducer.startdate");
    private String endDateTime = (Util.getProperty("corebos.restproducer.enddate").isEmpty()) ? System.getenv("FILTER_END_DATE") : Util.getProperty("corebos.restproducer.enddate");

    protected static org.apache.kafka.clients.producer.Producer<String, String> producer;
    protected RESTClient restClient;

    public RESTAPIProducer() throws Exception {
        restClient = new RESTClient(rest_api_url);
        Properties props = new Properties();
        props.put("metadata.broker.list", KAFKA_URL);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("request.required.acks", "1");
        producer = new KafkaProducer(props);
    }

    protected void publishMessage(String topic, String key, String message, int partition) {
        String runtime = new Date().toString();
        String msg = "Message Publishing Time - " + runtime + message;
//        System.out.println(msg);
        try {
            RecordMetadata metadata = (RecordMetadata) producer.send(new ProducerRecord(topic, partition, key, message)).get();
//            System.out.printf("Record sent with key %s to partition %d with offset " + metadata.offset() + " with value %s Time %s"
//                     , key, metadata.partition(), message, runtime);
//            System.out.println("topic = " + topic);
//            System.out.println("key = " + key);
//            System.out.println("message = " + message);
//            System.out.println("metadata.partition() = " + metadata.partition());
            System.out.println(msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void init() throws ParseException {

        /**
         * We send the first request with filter in which will act as the first round
         * pageSize=100
         * pageNr=1
         * */

        String isFirstRequest = Config.getInstance().isFirstRequest();


        if (isFirstRequest.isEmpty() || isFirstRequest.equals("YES")) {
            int pageSize = Integer.parseInt(Objects.requireNonNull(pagesize));
            // int pageSize = 25; //to Delete After Testing
            int pageNr = 1;

            Object response = doGet(restAPIKey, pageSize, pageNr, startDateTime, endDateTime, getDateForFiltering());

            if (response == null)
                return;

            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(response.toString());
            int totalNumberOfRecords = Integer.parseInt(jsonObject.get("nSpedizioniAccordingFilters").toString());
            // int totalNumberOfRecords = 100;//to Delete After Testing
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
                if (Integer.parseInt(Config.getInstance().getCurrentPartition()) % 13 == 0) {
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
        return (JSONArray) jsonObject.get("listaSpedizioni");
    }

    private Object doGet(String apiKey, int pageSize, int pageNumber, String startDateTime, String endDateTime, String currentDateTime) {
        Map<String, String> mapToSend = new HashMap<>();
        mapToSend.put("pageSize", String.valueOf(pageSize));
        mapToSend.put("pageNr", String.valueOf(pageNumber));
        if (!startDateTime.isEmpty() && !endDateTime.isEmpty()) {
            mapToSend.put("dataRegistrazioneEsitoDal", startDateTime);
            mapToSend.put("dataRegistrazioneEsitoAl", endDateTime);
        } else {
             mapToSend.put("dataRegistrazioneEsitoDal", currentDateTime);
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
        if (filterByCurrentDate.toString().equals("yes")) {
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDateTime currentDate = LocalDateTime.now();
            return dateTimeFormatter.format(currentDate);
        } else {
            return dateToFilterRecords;
        }
    }

    private void processResponseData(Object response, int currentPage) throws ParseException {
        System.out.println("Waiting for Result");
        if (response == null)
            return;
        JSONArray shipmentsData = getShipmentsData(response);
        JSONObject shipmentsStatus = getShipmentStatus(response);

        String partitionKey = "partition-" + currentPage;
        for (Object shipment : shipmentsData) {
            /* WeCombine Shipment and their Status together because Status(Process Log in CoreBos Application) are for the Shipment so
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

        /**for (Object key : shipmentsStatus.keySet()) {
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
