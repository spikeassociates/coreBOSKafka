package producer;

import helper.Config;
import helper.Util;
import model.KeyData;

import java.util.*;

public class SyncProducer extends Producer {
    public static final int timeIntervalMin = Integer.parseInt(Util.getProperty("corebos.syncproducer.timeIntervalMin") != null ? Util.getProperty("corebos.syncproducer.timeIntervalMin") : Util.dafaultTime);
    public static final String syncInitTimestamp = Util.getProperty("corebos.syncproducer.initialTimestamp") != null ? Util.getProperty("corebos.syncproducer.initialTimestamp") : "1568194862";

    public static final String UPDATED_KEY = "update_account";
    public static final String DELETED_KEY = "delete_account";

    private final String topic = "first_topic";

    public SyncProducer() throws Exception {
    }


    public void init() {

        Object response = doSync();
        if (response == null)
            return;
        List updatedList = getUpdated(response);
        List deletedList = getDeleted(response);

        for (Object updated : updatedList) {
            String moduleId = (String) wsClient.getModuleId("" + ((Map) updated).get("id"));
            if (!moduleMap.containsKey(moduleId))
                continue;

            String module = (String) ((Map) moduleMap.get(moduleId)).get("name");
            KeyData keyData = new KeyData();
            keyData.module = module;
            keyData.operation = Util.methodUPDATE;
            publishMessage(topic, Util.getJson(keyData), Util.getJson(updated));
        }
        for (Object deleted : deletedList) {
            KeyData keyData = new KeyData();
            keyData.operation = "delete";
            publishMessage(topic, Util.getJson(keyData), Util.getJson(deleted));
        }
        Config.getInstance().save();
        System.out.println("Producer Finished");
    }

    private Object doSync() {
        String modifiedTime = Config.getInstance().getLastTimeStampToSync();
        if (modifiedTime.equals(""))
            modifiedTime = syncInitTimestamp;
        Map<String, Object> mapToSend = new HashMap<>();
        mapToSend.put("modifiedTime", modifiedTime);
//        mapToSend.put("modifiedTime", "1568194862");
//        mapToSend.put("modifiedTime", "1569379878933");
//        mapToSend.put("modifiedTime", "1570105020");
        Object response = wsClient.doInvoke("sync", mapToSend);
        if (response == null)
            return response;
        long currentTime = new Date().getTime() / 1000;
        Config.getInstance().setLastTimeStampToSync("" + currentTime);
        return response;
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
