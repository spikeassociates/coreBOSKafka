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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import service.RESTClient;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static java.lang.System.exit;

public class RESTAPIProducer {
    public static final int timeIntervalMin = Integer.parseInt(Util.getProperty(
            "corebos.restproducer.timeIntervalMin") != null ? Objects.requireNonNull(Util.getProperty(
            "corebos.restproducer.timeIntervalMin")) : Util.dafaultTime);

    private final String topic = Util.getProperty("corebos.restproducer.topic");
    private final String rest_api_url = Util.getProperty("corebos.restproducer.url");
    private final String auth_endpoint = Util.getProperty("corebos.restproducer.authendpoint");
    private final String _endpoint = Util.getProperty("corebos.restproducer.endpoint");
    private final String key = Util.getProperty("corebos.restproducer.key");

    protected static final String username = Util.getProperty("corebos.restproducer.username");
    protected static final String password = Util.getProperty("corebos.restproducer.password");

    protected static final String KAFKA_URL = Util.getProperty("corebos.kafka.url");
    protected static org.apache.kafka.clients.producer.Producer<String, String> producer;

    protected RESTClient restClient;
    private final String pagesize = Util.getProperty("corebos.restproducer.pagesize");

    public RESTAPIProducer() throws Exception {
        restClient = new RESTClient(rest_api_url);
        String auth_credentials = "{\"username\": \"" + username + "\", \"password\": \"" + password + "\"}";
        System.out.println(auth_credentials);
        if (!restClient.doAuthorization(auth_credentials, auth_endpoint)) {
            throw new Exception("Authorization Error");
        }

        Properties props = new Properties();
        props.put("metadata.broker.list", KAFKA_URL);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("request.required.acks", "1");
        producer = new KafkaProducer(props);
    }

    protected void publishMessage(String topic, String key, String message) {
        String runtime = new Date().toString();
        String msg = "Message Publishing Time - " + runtime + message;
        //System.out.println(msg);
        try {
            RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key, message)).get();
            //System.out.printf("Record sent with key %s to partition %d with offset " + metadata.offset() + " with value %s Time %s"
            //        , key, metadata.partition(), message, runtime);
            //System.out.println("topic = " + topic);
            // System.out.println("key = " + key);
            // System.out.println("message = " + message);
            //System.out.println("metadata.partition() = " + metadata.partition());
            //System.out.println(msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void init() throws ParseException {

        /*
         * We send the first request with filter in which will act as the first round
         * pageSize=100
         * pageNr=1
         * */

        String isFirstRequest = Config.getInstance().isFirstRequest();
        String startDateTime = "", endDateTime = "";


        if (isFirstRequest.isEmpty() || isFirstRequest.equals("YES")) {
            int pageSize = Integer.parseInt(Objects.requireNonNull(pagesize));
            // int pageSize = 25; //to Delete After Testing
            int pageNr = 1;

            Object response = doGet(restClient.get_servicetoken(), pageSize, pageNr, startDateTime, endDateTime, getTodayDate());

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
                    // exit(0);
                } else {
                    init();
                }
            }

        } else {
            int pageSize = Integer.parseInt(Objects.requireNonNull(pagesize));
            int savedPageNumbers = Integer.parseInt(Config.getInstance().getTotalNumberOfPages());
            Config.getInstance().setFirstRequest("" + "YES");
            for (int page = 2; page <= savedPageNumbers; page++) {
                Object response = doGet(restClient.get_servicetoken(), pageSize, page, startDateTime, endDateTime, getTodayDate());
                if (Integer.parseInt(Config.getInstance().getCurrentPartition()) % 10 == 0) {
                    processResponseData(response, 1);
                    Config.getInstance().setCurrentPartition("1"); // reset partition
                } else {
                    // processResponseData(response, page);
                    processResponseData(response, Integer.parseInt(Config.getInstance().getCurrentPartition()) + 1);
                    Config.getInstance().setCurrentPartition(String.valueOf((Integer.parseInt(Config.getInstance().getCurrentPartition()) + 1)));
                }

            }
            producer.close();
            // exit(0);
        }

    }

    private JSONObject getShipmentStatus(Object response) throws ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(response.toString());
        return (JSONObject) jsonObject.get("mappaEsitiPerSpedizione");
    }

    private List getShipmentsData(Object response) throws ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(response.toString());
        return (List) jsonObject.get("listaSpedizioni");
    }

    private Object doGet(String token, int pageSize, int pageNumber, String startDateTime, String endDateTime, String currentDateTime) {
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
        headersArray[1] = new BasicHeader("Authorization", token);
        // System.out.println(Arrays.toString(headersArray));
        Object response = restClient.doGet(_endpoint, mapToSend, headersArray);
        if (response == null)
            return null;
        long currentTime = new Date().getTime() / 1000;
        Config.getInstance().setLastTimeStampToSync("" + currentTime);
        return response;
    }

    private String getTodayDate() {
        // DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDateTime currentDate = LocalDateTime.now();
        return dateTimeFormatter.format(currentDate);
    }

    private void processResponseData(Object response, int currentPage) throws ParseException {
        System.out.println("Waiting for Result");
        if (response == null)
            return;
        List shipmentsData = getShipmentsData(response);
        JSONObject shipmentsStatus = getShipmentStatus(response);

        String partitionKey = "partition-" + currentPage;
        for (Object shipment : shipmentsData) {
            String module = "Shipments";
            KeyData keyData = new KeyData();
            keyData.module = module;
            keyData.operation = Util.methodUPDATE;
            // System.out.println(Util.getJson(shipment));
            JSONObject messageToSend = new JSONObject();
            messageToSend.put("operation", keyData);
            messageToSend.put("data", shipment);
            // publishMessage(topic, Util.getJson(keyData), Util.getJson(shipment));
            publishMessage(topic, partitionKey, Util.getJson(messageToSend));
        }

        for (Object key : shipmentsStatus.keySet()) {
            String shipmentid = (String) key;
            Object status = shipmentsStatus.get(shipmentid);
            String module = "ProcessLog";
            KeyData keyData = new KeyData();
            keyData.module = module;
            keyData.operation = Util.methodUPDATE;
            JSONObject singleStatus = new JSONObject();
            singleStatus.put(shipmentid, status);
            // System.out.println(Util.getJson(singleStatus));
            JSONObject messageToSend = new JSONObject();
            messageToSend.put("operation", keyData);
            messageToSend.put("data", singleStatus);
            // publishMessage(topic, Util.getJson(keyData), Util.getJson(singleStatus));
            publishMessage(topic, partitionKey, Util.getJson(messageToSend));
        }

        Config.getInstance().save();
    }
}
