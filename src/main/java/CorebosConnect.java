import helper.Util;
import producer.SyncProducer;
import vtwslib.WSClient;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CorebosConnect {
    public static final String COREBOS_URL = Util.getProperty("corebos.cbtest.url");
    public static final String USERNAME = Util.getProperty("corebos.cbtest.username");
    public static final String ACCESS_KEY = Util.getProperty("corebos.cbtest.access_key");
    public static final String Modules = Util.getProperty("corebos.cbtest.modules");


    public static WSClient wsClient = new WSClient(COREBOS_URL);


    public static void main(String[] args) {
        boolean isLogin = wsClient.doLogin(USERNAME, ACCESS_KEY);
        System.out.println("isLogin = " + isLogin);
        if (!isLogin)
            return;
        System.out.println("getModules() = " + getModules());
        upsertContact();
    }


    public static Map getModules() {
        Map o = wsClient.doDescribe(Modules);
        Object[] collections = o.values().toArray();
        Map emptyMap = new HashMap();

        for (Object collectionO : collections) {
            Map theMap = (Map) collectionO;
            emptyMap.put(theMap.get("idPrefix"), theMap);
        }
        return emptyMap;
    }

    public static void selectQuery() {

        Object d = wsClient.doQuery("select * from Contacts where lastname like 'Sarja';");
        System.out.println(Util.getJson(d));


    }

    public static void createContact() {
        Map<String, Object> mapToSend = new HashMap<>();
        Map<String, Object> element = new HashMap<>();

        element.put("lastname", "SarjaTest1");
        element.put("firstname", "ArditTest");
        element.put("otherzip", "123456");
        element.put("assigned_user_id", wsClient.getUserID());
        element.put("homephone", "0693700874");
        element.put("mobile", "0693700844");

        mapToSend.put("elementType", Util.elementTypeCONTACTS);
        mapToSend.put("element", Util.getJson(element));


        Object d = wsClient.doInvoke(Util.methodCREATE, mapToSend, "POST");
        System.out.println("Util.getJson(d) = " + Util.getJson(d));

    }

    public static void upsertContact() {
        Map<String, Object> mapToSend = new HashMap<>();
        Map<String, Object> element = new HashMap<>();

        element.put("lastname", "Sarja");
        element.put("firstname", "Ardit");
        element.put("otherzip", "123456");
        element.put("assigned_user_id", wsClient.getUserID());
        element.put("homephone", "0693700874");
        element.put("mobile", "141");

        mapToSend.put("elementType", Util.elementTypeCONTACTS);
        mapToSend.put("element", Util.getJson(element));
        mapToSend.put("searchOn", "mobile");


        Object d = wsClient.doInvoke(Util.methodUPSERT, mapToSend, "POST");
        System.out.println("Util.getJson(d) = " + Util.getJson(d));

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
        element.put("mobile", "123456789");

        mapToSend.put("elementType", Util.elementTypeCONTACTS);
        mapToSend.put("element", Util.getJson(element));


        Object d = wsClient.doInvoke(Util.methodUPDATE, mapToSend, "POST");
        System.out.println("Util.getJson(d) = " + Util.getJson(d));

    }

    public static void createAccount() {
        Map<String, Object> mapToSend = new HashMap<>();
        Map<String, Object> element = new HashMap<>();

        element.put("accountname", "arditsarjaTestAccount6");
        element.put("website", "arditsarjaTestAccount6");
        element.put("assigned_user_id", wsClient.getUserID());

        mapToSend.put("elementType", Util.elementTypeACCOUNTS);
        mapToSend.put("element", Util.getJson(element));


        Object d = wsClient.doInvoke(Util.methodCREATE, mapToSend, "POST");
        System.out.println(Util.getJson(d));

    }

    public static void updateAccount() {
        Map<String, Object> mapToSend = new HashMap<>();
        Map<String, Object> element = new HashMap<>();

        element.put("id", "11x44874");
        element.put("assigned_user_id", wsClient.getUserID());
        element.put("phone", "0693700884");
        element.put("email1", "arditsarja@gmail.com");
        element.put("fax", "0693755584");
        element.put("accountname", "arditsarjaTEstdfd");
        element.put("test", "test");

        mapToSend.put("elementType", Util.elementTypeACCOUNTS);
        mapToSend.put("element", Util.getJson(element));


        Object d = wsClient.doInvoke(Util.methodUPDATE, mapToSend, "POST");
        if (d == null)
            System.out.println("wsClient.lastError() = " + wsClient.lastError());
        System.out.println("Util.getJson(d) = " + Util.getJson(d));

    }

    public static void sync() {

        long modifiedTime = (new Date().getTime() - (long) SyncProducer.timeIntervalMin * 60 * 1000) / 1000;
        Map<String, Object> mapToSend = new HashMap<>();
//        mapToSend.put("modifiedTime", "" + modifiedTime);
//        mapToSend.put("modifiedTime", "1568194862");
//        mapToSend.put("modifiedTime", "1569925502");
        mapToSend.put("modifiedTime", "1570105020");

        Object d1 = wsClient.doInvoke("sync", mapToSend);
        Util.createJSonFile(d1, "sync");
        System.out.println(Util.getJson(d1));

    }

    public static void syncUpdated() {

        long modifiedTime = (new Date().getTime() - (long) SyncProducer.timeIntervalMin * 60 * 1000) / 1000;
        Map<String, Object> mapToSend = new HashMap<>();
//        mapToSend.put("modifiedTime", "" + modifiedTime);
//        mapToSend.put("modifiedTime", "1568194862");
        mapToSend.put("modifiedTime", "1569925502");

        Object d1 = wsClient.doInvoke("sync", mapToSend);
        List updated = (List) ((Map) d1).get("updated");
        for (int i = 0; i < updated.size(); i++) {
            Object update = updated.get(i);
            Object id = ((Map) update).get("id");
            Object o = wsClient.doRetrieve(id);
            Util.createJSonFile(update, "updated" + (i + 1));
            Util.createJSonFile(o, "updatedRetreive" + (i + 1));
        }
        Util.createJSonFile(updated, "updated");
        System.out.println(Util.getJson(d1));

    }

    public static void syncDeleted() {

        long modifiedTime = (new Date().getTime() - (long) SyncProducer.timeIntervalMin * 60 * 1000) / 1000;
        Map<String, Object> mapToSend = new HashMap<>();
//        mapToSend.put("modifiedTime", "" + modifiedTime);
//        mapToSend.put("modifiedTime", "1568194862");
        mapToSend.put("modifiedTime", "1569925502");

        Object d1 = wsClient.doInvoke("sync", mapToSend);
        List updated = (List) ((Map) d1).get("deleted");
        for (int i = 0; i < updated.size(); i++) {
            Object id = updated.get(i);
            Object o = wsClient.doRetrieve(id);
            Util.createJSonFile(o, "deleted" + (i + 1));
        }
        Util.createJSonFile(updated, "deleted");
        System.out.println(Util.getJson(d1));

    }

    public static void doRetrive() {


//        Object d1 = wsClient.doRetrieve("12x44885");
        Object d1 = wsClient.doRetrieve("12x44873");
        Util.createJSonFile(d1, "Retrieved");
        System.out.println(Util.getJson(d1));

    }
}