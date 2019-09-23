package producer;

import Helper.Util;

import java.util.*;

public class SyncProducer extends Producer {
    public static final int timeIntervalMin = Integer.parseInt(Util.getProperty("corebos.syncproducer.timeIntervalMin") != null ? Util.getProperty("corebos.syncproducer.timeIntervalMin") : Util.dafaultTime);

    public static final String UPDATED_KEY = "update_account";
    public static final String DELETED_KEY = "delete_account";

    private final String topic = "first_topic";


    public void init() {

        Object response = doSync();
        List updatedList = getUpdated(response);
        List deletedList = getDeleted(response);

        for (Object updated : updatedList) {
            publishMessage(topic, UPDATED_KEY, Util.getJson(updated));
        }
        for (Object deleted : deletedList) {
            publishMessage(topic, DELETED_KEY, Util.getJson(deleted));
        }

        System.out.println(" Util.getJson(response) = " + Util.getJson(response));
        publishMessage(topic, DELETED_KEY, DEFAULT_VALUE + new Date().toString() + "   " + Util.getJson(response));
    }

    private Object doSync() {

        long modifiedTime = new Date().getTime() - (long) timeIntervalMin * 60 * 1000;
        Map<String, Object> mapToSend = new HashMap<>();
        mapToSend.put("modifiedTime", "" + modifiedTime);
//        mapToSend.put("modifiedTime", "1568194862");
        return wsClient.doInvoke("sync", mapToSend);
    }

    private List getUpdated(Object o) {
        if (!(o instanceof Map)) {
            return new ArrayList();
        }
        return (List) ((Map) o).get("updated");
    }

    private List getDeleted(Object o) {
        if (!(o instanceof Map)) {
            return null;
        }
        return (List) ((Map) o).get("deleted");
    }


}
