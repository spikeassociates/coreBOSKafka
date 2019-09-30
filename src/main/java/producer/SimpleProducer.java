package producer;

import helper.Util;
import model.ValueData;

import java.io.File;

public class SimpleProducer extends Producer {
    public static final int timeIntervalMin = Integer.parseInt(Util.getProperty("corebos.simpleproducer.timeIntervalMin") != null ? Util.getProperty("corebos.simpleproducer.timeIntervalMin") : Util.dafaultTime);

    private String key = "mykey";
    private String value = "Ardit";
    private String topic = "simple_topic";

    public SimpleProducer() throws Exception {
    }


    public void init() {
        ValueData valueData = Util.getObjectFromJson(new File("C:\\Users\\User\\Desktop\\corebos\\src\\test\\file_test\\jsonFile.json"), ValueData.class);

        publishMessage(topic, key, Util.getJson(valueData));
    }
}
