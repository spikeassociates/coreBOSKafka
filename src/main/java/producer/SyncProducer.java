package producer;

import Helper.Util;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SyncProducer extends Producer {
    public static final int timeIntervalMin = Integer.parseInt(Util.getProperty("corebos.syncproducer.timeIntervalMin") != null ? Util.getProperty("corebos.syncproducer.timeIntervalMin") : Util.dafaultTime);

    private String key = "mykey";
    private String value = "Ardit";
    private String topic = "first_topic";


    public void init() {

        Object response = doSync();
        publishMessage(topic, key, value + new Date().toString() + "   " + Util.getJson(response));
    }

    private Object doSync() {

        Date date = new Date();
        long modifiedTime = date.getTime() - (long) timeIntervalMin * 60 * 1000;

        Map<String, Object> mapToSend = new HashMap<>();
        mapToSend.put("modifiedTime", "" + modifiedTime);
        Object d1 = wsClient.doInvoke("sync", mapToSend);
        return d1;
    }
}
