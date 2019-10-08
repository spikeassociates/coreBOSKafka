package producer;

import helper.Util;
import model.ValueData;
import stream.Stream;

import java.io.File;

public class StreamProducer extends Producer {
    public static final int timeIntervalMin = Integer.parseInt(Util.getProperty("corebos.simpleproducer.timeIntervalMin") != null ? Util.getProperty("corebos.simpleproducer.timeIntervalMin") : Util.dafaultTime);

    private String key = "theKey";
    private String value = "ArditShpetimSarja";
    private String topic = Stream.firstStreamTopic;

    public StreamProducer() throws Exception {
    }


    public void init() {
        ValueData valueData = Util.getObjectFromJson(new File("C:\\Users\\User\\Desktop\\corebos\\src\\test\\file_test\\jsonFile.json"), ValueData.class);

        publishMessage(topic, key, Util.getJson(valueData));
//        publishMessage(topic, key, value);

    }

    public static void main(String[] args) {
        start();
    }

    public static void start() {
        try {
            new StreamProducer().init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
