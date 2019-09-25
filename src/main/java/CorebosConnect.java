import Helper.Util;
import vtwslib.WSClient;

import java.util.HashMap;
import java.util.Map;


public class CorebosConnect {
    public static final String COREBOS_URL = Util.getProperty("corebos.url");
    //    public static final String COREBOS_URL = Util.getProperty("corebos.test.url");
    public static final String USERNAME = Util.getProperty("corebos.username");
    public static final String ACCESS_KEY = Util.getProperty("corebos.access_key");


    public static WSClient wsClient = new WSClient(COREBOS_URL);


    public static void main(String[] args) {

        System.out.println("wsClient = " + wsClient.doLogin(USERNAME, ACCESS_KEY));

        createAccount();
        sync();
        selectQuery();

    }

    public static void selectQuery() {

        Object d = wsClient.doQuery("select * from Contacts where lastname='Sarja';");
        System.out.println(Util.getJson(d));


    }

    public static void update() {
        Map<String, Object> mapToSend = new HashMap<>();
        Map<String, Object> element = new HashMap<>();

        element.put("accountname", "arditsarja1");
        element.put("assigned_user_id", "19x1");
        element.put("email1", "ardit.sarja94@gmail.com");
//        element.put("homephone", "0693700844");

        mapToSend.put("elementType", Util.ACCOUNTS);
        mapToSend.put("element", Util.getJson(element));


        Object d = wsClient.doInvoke("update", mapToSend, "POST");

    }

    public static void createAccount() {
        Map<String, Object> mapToSend = new HashMap<>();
        Map<String, Object> element = new HashMap<>();

        element.put("accountname", "coreBOSwsTest");
        element.put("assigned_user_id", "19x1");

        mapToSend.put("elementType", "Accounts");
        mapToSend.put("element", Util.getJson(element));


        Object d = wsClient.doInvoke("create", mapToSend, "POST");
        System.out.println(Util.getJson(d));

    }

    public static void sync() {

        Map<String, Object> mapToSend = new HashMap<>();
        mapToSend.put("modifiedTime", "1569379878");

        Object d1 = wsClient.doInvoke("sync", mapToSend);
        System.out.println(Util.getJson(d1));

    }
}