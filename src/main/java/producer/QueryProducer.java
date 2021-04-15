package producer;

import helper.Config;
import helper.Util;
import model.KeyData;

import java.util.*;

public class QueryProducer extends Producer {
    public static final int timeIntervalMin = Integer.parseInt(Util.getProperty(
            "corebos.queryproducer.timeIntervalMin") != null ? Objects.requireNonNull(Util.getProperty(
                    "corebos.queryproducer.timeIntervalMin")) : Util.dafaultTime);

    private final String topic = Util.getProperty("corebos.queryproducer.topic");
    private final String query = Util.getProperty("corebos.queryproducer.query");

    public QueryProducer() throws Exception {
    }

    public void init() {
        Object response = doQuery();
        if (response == null)
            return;
        List updatedList = (List) response;

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

    }

    private Object doQuery() {
        Object response = wsClient.doQuery(query, 3);
        if (response == null)
            return null;
        long currentTime = new Date().getTime() / 1000;
        Config.getInstance().setLastTimeStampToSync("" + currentTime);
        return response;
    }
}
