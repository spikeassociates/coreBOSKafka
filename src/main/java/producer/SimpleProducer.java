package producer;

import Helper.Util;

public class SimpleProducer extends Producer {
    public static final int timeIntervalMin = Integer.parseInt(Util.getProperty("corebos.simpleproducer.timeIntervalMin") != null ? Util.getProperty("corebos.simpleproducer.timeIntervalMin") : Util.dafaultTime);

    private String key = "mykey";
    private String value = "Ardit";
    private String topic = "first_topic";


    public void init() {
        publishMessage(topic, key, value);
    }
}
