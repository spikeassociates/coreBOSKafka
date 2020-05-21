package service;

import org.apache.http.Header;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import vtwslib.HTTP_Client;

import java.util.Map;

public class RESTClient {
    String _servicebaseurl;
    HTTP_Client _client;
    String _serviceurl;
    String _serviceuser;
    String _servicetoken;
    Object _lasterror;

    public RESTClient(String _servicebaseurl) {
        this._servicebaseurl = _servicebaseurl;
    }

    public JSONArray doGet(String endPoint, Map<String, String> queryParameters, Header[] headerParameters, String key) {
        String urlToCall = this._servicebaseurl + endPoint;
        this._client = new HTTP_Client(urlToCall);
        System.out.println(urlToCall);
        // queryParameters.put("codiceClienteOrdinante", "EPRICE");
        // queryParameters.put("pageSize", "10");
       // queryParameters.put("ldv", "624");
        Object response = this._client.doGet(queryParameters, true, headerParameters);
        // System.out.println(response);
        // if (this.hasError(response)) {
              // return null;
        // } else {
            // JSONArray result = (JSONArray) ((JSONObject) response).get(key);
            // System.out.println(response);
            JSONObject result = (JSONObject) (response);
            return (JSONArray) result.get(key);
       // }
    }

    public JSONObject doGet(String endPoint, Map<String, String> queryParameters, Header[] headerParameters) {
        String urlToCall = this._servicebaseurl + endPoint;
        this._client = new HTTP_Client(urlToCall);
        System.out.println(urlToCall);
        // queryParameters.clear();
        // queryParameters.put("codiceClienteOrdinante", "EPRICE");
        // queryParameters.put("pageSize", "10");
        // queryParameters.put("ldv", "262");
        Object response = this._client.doGet(queryParameters, true, headerParameters);
        return (JSONObject) (response);
    }


    public boolean hasError(Object result) {
        boolean isError = false;

        try {
            if (result == null) {
                isError = true;
            } else if (result instanceof Exception) {
                this._lasterror = ((Exception) result).getMessage();
                isError = true;
            } else if (result instanceof JSONObject) {
                JSONObject resultObject = (JSONObject) ((JSONObject) result).get("status");
                if (!resultObject.get("message").toString().equals("Success")) {
                    this._lasterror = resultObject.get("error");
                    isError = true;
                }
            }
        } catch (Exception var4) {
            ;
        }

        return isError;
    }

    public Object lastError() {
        return this._lasterror;
    }

    public boolean doAuthorization(String auth_credentials, String auth_endpoint) {
        String urlToCall = this._servicebaseurl + auth_endpoint;
        this._client = new HTTP_Client(urlToCall);
        Object response = this._client.doPost(auth_credentials, true);
        System.out.println(response);
        /*{
            "status": {
            "code": 200,
                    "message": "Success"
        },
            "token": "ff032afadc042091ca576a9ae8a9f0a2"
        }*/
        if (this.hasError(response)) {
            // First Retry for network Connection
            response = this._client.doPost(auth_credentials, true);
            if (!this.hasError(response)) {
                this._servicetoken = (String)((JSONObject) response).get("token");
                return true;
            } else {
                // Second Retry for Network Connection
                response = this._client.doPost(auth_credentials, true);
                if (!this.hasError(response)) {
                    this._servicetoken = (String)((JSONObject) response).get("token");
                    return true;
                } else {
                    // Third Retry for Network Connection
                    response = this._client.doPost(auth_credentials, true);
                    if (!this.hasError(response)) {
                        this._servicetoken = (String)((JSONObject) response).get("token");
                        return true;
                    } {
                        return false;
                    }
                }
            }
        } else {
            this._servicetoken = (String)((JSONObject) response).get("token");
            return true;
        }
    }

    public String get_servicetoken() {
        return _servicetoken;
    }
}