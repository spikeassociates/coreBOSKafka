import HElper.WSClient;

public class CorebosConnect {
//    public static final String COREBOS_URL = "http://localhost/corebos";
    public static final String COREBOS_URL = "http://localhost/wsdevel";
    public static final String USERNAME = "admin";
    public static final String ACCESS_KEY = "cdYTBpiMR9RfGgO";

    public static void main(String[] args) {
        WSClient wsClient = new WSClient(COREBOS_URL);
        wsClient.doLogin(USERNAME,ACCESS_KEY);
        System.out.println(wsClient.doListTypes());
    }
}