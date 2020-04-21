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
import service.RESTClient;

import java.util.*;
import java.util.concurrent.ExecutionException;

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
    public RESTAPIProducer() throws Exception {
        restClient = new RESTClient(rest_api_url);
        String auth_credentials = "{\"username\": \""+username+"\", \"password\": \""+password+"\"}";
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
        System.out.println(msg);
        try {
            RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key, message)).get();
            System.out.printf("Record sent with key %s to partition %d with offset " + metadata.offset() + " with value %s Time %s"
                    , key, metadata.partition(), message, runtime);
            System.out.println("topic = " + topic);
            System.out.println("key = " + key);
            System.out.println("message = " + message);
            System.out.println("metadata.partition() = " + metadata.partition());
            System.out.println(msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void init() {

        Object response = doGet(restClient.get_servicetoken());
        System.out.println("Waiting for Result");
        if (response == null)
            return;
        List updatedList = getUpdated(response);
        System.out.println(updatedList);


        for (Object updated : updatedList) {
//            String moduleId = (String) wsClient.getModuleId("" + ((Map) updated).get("id"));
//            if (!moduleMap.containsKey(moduleId))
//                continue;

//            String module = (String) ((Map) moduleMap.get(moduleId)).get("name");
            // TODO: 4/10/20 The Module Name Should be Dynamic nat hardcoded value
            String module = "Shipments";
            KeyData keyData = new KeyData();
            keyData.module = module;
            keyData.operation = Util.methodUPDATE;
            System.out.println(Util.getJson(updated));
            publishMessage(topic, Util.getJson(keyData), Util.getJson(updated));
        }
        Config.getInstance().save();
        System.out.println("Producer Finished");
    }

    private Object doGet(String token) {
        String modifiedTime = Config.getInstance().getLastTimeStampToSync();
//        if (modifiedTime.equals(""))
//            modifiedTime = syncInitTimestamp;
        Map<String, String> mapToSend = new HashMap<>();
        //mapToSend.put("modifiedTime", modifiedTime);
        Header[] headersArray = new Header[2];
        headersArray[0] = new BasicHeader("Content-type", "application/json");
        headersArray[1] = new BasicHeader("Authorization", token);
        System.out.println(Arrays.toString(headersArray));
        Object response = restClient.doGet(_endpoint, mapToSend, headersArray,key);
        if (response == null)
            return null;
        long currentTime = new Date().getTime() / 1000;
        Config.getInstance().setLastTimeStampToSync("" + currentTime);
        return response;
    }

    private List getUpdated(Object o) {
//        if (!(o instanceof Map)) {
//            return new ArrayList();
//        }
        return (List) o;
    }
}