import Helper.Util;
import vtwslib.WSClient;

import java.util.HashMap;
import java.util.Map;


public class CorebosConnect {
    public static final String COREBOS_URL = Util.getProperty("corebos.url");
    public static final String USERNAME = Util.getProperty("corebos.username");
    public static final String ACCESS_KEY = Util.getProperty("corebos.access_key");

    public static void main(String[] args) {


        Map<String, Object> mapToSend = new HashMap<>();
        Map<String, Object> element = new HashMap<>();

        element.put("accountname", "coreBOSwsTest");
        element.put("assigned_user_id", "19x1");

        mapToSend.put("elementType", "Accounts");
        mapToSend.put("element", Util.getJson(element));



        WSClient wsClient = new WSClient(COREBOS_URL);
        wsClient.doLogin(USERNAME, ACCESS_KEY);
        Object d = wsClient.doInvoke("create", mapToSend, "POST");
        System.out.println(Util.getJson(d));
        mapToSend.clear();
        mapToSend.put("modifiedTime","1568194862");

        Object d1 = wsClient.doInvoke("sync", mapToSend);
        System.out.println(Util.getJson(d1));


        System.out.println(wsClient.doListTypes());
    }
}