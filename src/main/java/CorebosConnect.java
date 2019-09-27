import Helper.Util;
import producer.SyncProducer;
import vtwslib.WSClient;

import java.util.Date;
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


    }

    public static void selectQuery() {

        Object d = wsClient.doQuery("select * from Contacts where lastname like 'Sarja';");
        System.out.println(Util.getJson(d));


    }

    public static void updateContact() {
        Map<String, Object> mapToSend = new HashMap<>();
        Map<String, Object> element = new HashMap<>();

        element.put("lastname", "Sarja");
        element.put("firstname", "Ardit");
        element.put("otherzip", "1001");
        element.put("assigned_user_id", wsClient.getUserID());
        element.put("id", "12x44873");
        element.put("testColumt", "create by ardit");
        element.put("homephone", "0693700874");
        element.put("mobile", "0693700844");

        mapToSend.put("elementType", Util.elementTypeCONTACTS);
        mapToSend.put("element", Util.getJson(element));


        Object d = wsClient.doInvoke(Util.methodUPDATE, mapToSend, "POST");
        System.out.println("Util.getJson(d) = " + Util.getJson(d));

    }

    public static void updateAccount() {
        Map<String, Object> mapToSend = new HashMap<>();
        Map<String, Object> element = new HashMap<>();

        element.put("id", "11x44874");
        element.put("assigned_user_id", wsClient.getUserID());
        element.put("phone", "0693700884");
        element.put("email1", "arditsarja@gmail.com");
        element.put("fax", "0693700884");
        element.put("accountname", "arditsarjaTEstdfd");
        element.put("test", "test");

        mapToSend.put("elementType", Util.elementTypeACCOUNTS);
        mapToSend.put("element", Util.getJson(element));


        Object d = wsClient.doInvoke(Util.methodUPDATE, mapToSend, "POST");
        if (d == null)
            System.out.println("wsClient.lastError() = " + wsClient.lastError());
        System.out.println("Util.getJson(d) = " + Util.getJson(d));

    }

    public static void createAccount() {
        Map<String, Object> mapToSend = new HashMap<>();
        Map<String, Object> element = new HashMap<>();

        element.put("accountname", "arditsarjaTEstdfd");
        element.put("assigned_user_id", wsClient.getUserID());

        mapToSend.put("elementType", Util.elementTypeACCOUNTS);
        mapToSend.put("element", Util.getJson(element));


        Object d = wsClient.doInvoke(Util.methodCREATE, mapToSend, "POST");
        System.out.println(Util.getJson(d));

    }

    public static void sync() {

        long modifiedTime = (new Date().getTime() - (long) SyncProducer.timeIntervalMin * 60 * 1000) / 1000;
        Map<String, Object> mapToSend = new HashMap<>();
        mapToSend.put("modifiedTime", "" + modifiedTime);
//        mapToSend.put("modifiedTime", "1569379878");

        Object d1 = wsClient.doInvoke("sync", mapToSend);
        System.out.println(Util.getJson(d1));

    }
    public static void dorRetrive() {



        Object d1 = wsClient.doRetrieve( "11x44874");
        System.out.println(Util.getJson(d1));

    }
}