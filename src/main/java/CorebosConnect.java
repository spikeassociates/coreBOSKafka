import Helper.Util;
import vtwslib.WSClient;


public class CorebosConnect {
    public static final String COREBOS_URL = Util.getProperty("corebos.url");
    public static final String USERNAME =  Util.getProperty("corebos.username");
    public static final String ACCESS_KEY =  Util.getProperty("corebos.access_key");

    public static void main(String[] args) {

        WSClient wsClient = new WSClient(COREBOS_URL);
        wsClient.doLogin(USERNAME,ACCESS_KEY);
        System.out.println(wsClient.doListTypes());
    }
}