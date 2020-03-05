package producer;

import helper.Util;
import kafka.SiaeKeyData;
import model.ValueData;

import java.io.File;
import java.util.Date;

public class SimpleProducer extends Producer {
    public static final int timeIntervalMin = Integer.parseInt(Util.getProperty("corebos.simpleproducer.timeIntervalMin") != null ? Util.getProperty("corebos.simpleproducer.timeIntervalMin") : Util.dafaultTime);
    static int index = 0;
    private String key = "mykey";
    private String value = "Ardit";
    private String topic = "sign_topic";

    public SimpleProducer() throws Exception {
    }


    public void init() {
        ValueData valueData = Util.getObjectFromJson(new File("C:/Users/User/Desktop/corebos/src/test/file_test/jsonFile.json"), ValueData.class);
        SiaeKeyData siaeKeyData = new SiaeKeyData();
        siaeKeyData.username = "Ardoit";
        siaeKeyData.module = "futjakot";
        siaeKeyData.nameOrId = "" + index;
        index++;
        siaeKeyData.date = new Date().toString();
        publishMessage(topic, Util.getJson(siaeKeyData), Util.getJson(valueData));
    }

    public static void main(String[] args) throws Exception {
        new SimpleProducer().init();
    }
}
