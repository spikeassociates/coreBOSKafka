package vtwslib;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.*;

public class WSClient {
    String _servicebase = "webservice.php";
    HTTP_Client _client;
    String _serviceurl;
    String _serviceuser;
    String _servicekey;
    String _servertime;
    String _expiretime;
    String _servicetoken;
    String _sessionid;
    Object _userid;
    Object _lasterror;


    public void set_sessionid(String _sessionid) {
        this._sessionid = _sessionid;
    }

    public void set_userid(Object _userid) {
        this._userid = _userid;
    }

    public WSClient(String url) {
        this._serviceurl = this.getWebServiceURL(url);
        this._client = new HTTP_Client(this._serviceurl);
    }

    public void reinitailize() {
        this._client = new HTTP_Client(this._serviceurl);
    }

    protected String getWebServiceURL(String url) {
        if (!url.endsWith("/")) {
            url = url + "/";
        }

        return url + this._servicebase;
    }

    public Object getId(String id) {
        String[] splits = id.split("x");
        return splits[1];
    }

    public Object getModuleId(String id) {
        String[] splits = id.split("x");
        return splits[0];
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
                JSONObject resultObject = (JSONObject) result;
                if (resultObject.get("success").toString() == "false") {
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

    protected boolean __doChallenge(String username) {
        Map getdata = new HashMap();
        getdata.put("operation", "getchallenge");
        getdata.put("username", username);
        Object response = this._client.doGet(getdata, true);
        if (this.hasError(response)) {
            return false;
        } else {
            JSONObject result = (JSONObject) ((JSONObject) response).get("result");
            this._servertime = result.get("serverTime").toString();
            this._expiretime = result.get("expireTime").toString();
            this._servicetoken = result.get("token").toString();
            return true;
        }
    }

    protected void checkLogin() {
//        boolean loggedIn = true;
//        long _expiretime = Long.parseLong(this._expiretime);
//        long _timenow = new Date().getTime() / 1000;
//        if (_timenow >= _expiretime) {
//            loggedIn = doLogin(_serviceuser, _servicekey);
//        }
//        if (!loggedIn) {
//            try {
//                throw new Exception("Login error");
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
    }

    protected String md5Hex(String input) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] hash = md.digest(input.getBytes());
        return (new BigInteger(1, hash)).toString(16);
    }

    public Object toJSON(String input) {
        return this._client.__jsondecode(input);
    }

    public String toJSONString(Object input) {
        return this._client.__jsonencode(input);
    }

    public boolean doLogin(String username, String vtigerUserAccessKey) {
        System.out.println("Logging in");
        System.out.println("_serviceurl = " + _serviceurl);
        System.out.println("_client = " + _client);
        System.out.println("username = " + username);
        System.out.println("vtigerUserAccessKey = " + vtigerUserAccessKey);
        if (!this.__doChallenge(username)) {
            System.out.println("Challenge Failed!");
            return false;
        } else {
            try {
                Map postdata = new HashMap();
                postdata.put("operation", "login");
                postdata.put("username", username);
                postdata.put("accessKey", this.md5Hex(this._servicetoken + vtigerUserAccessKey));
                Object response = this._client.doPost(postdata, true);
                if (this.hasError(response)) {
                    System.out.println("response = " + response);
                    return false;
                } else {
                    JSONObject result = (JSONObject) ((JSONObject) response).get("result");
                    this._serviceuser = username;
                    this._servicekey = vtigerUserAccessKey;
                    this._sessionid = result.get("sessionName").toString();
                    this._userid = result.get("userId").toString();
                    return true;
                }
            } catch (Exception var6) {
                this.hasError(var6);
                return false;
            }
        }
    }

    public JSONArray doQuery(String query) {
        this.checkLogin();
        if (!query.trim().endsWith(";")) {
            query = query + ";";
        }

        Map getdata = new HashMap();
        getdata.put("operation", "query");
        getdata.put("sessionName", this._sessionid);
        getdata.put("query", query);
        Object response = this._client.doGet(getdata, true);
        if (this.hasError(response)) {
            return null;
        } else {
            JSONArray result = (JSONArray) ((JSONObject) response).get("result");
            return result;
        }
    }

    public List getResultColumns(JSONArray result) {
        List columns = new ArrayList();
        if (!result.isEmpty()) {
            JSONObject row = (JSONObject) result.get(0);
            Iterator iterator = row.keySet().iterator();

            while (iterator.hasNext()) {
                columns.add(iterator.next().toString());
            }
        }

        return columns;
    }

    public Map doListTypes() {
        this.checkLogin();
        Map getdata = new HashMap();
        getdata.put("operation", "listtypes");
        getdata.put("sessionName", this._sessionid);
        Object response = this._client.doGet(getdata, true);
        if (this.hasError(response)) {
            return null;
        } else {
            JSONObject result = (JSONObject) ((JSONObject) response).get("result");
            JSONArray resultTypes = (JSONArray) result.get("types");
            Map returnvalue = new HashMap();
            Iterator iterator = resultTypes.iterator();

            while (iterator.hasNext()) {
                Object value = iterator.next();
                Map returnpart = new HashMap();
                returnpart.put("name", value.toString());
                returnvalue.put(value, returnpart);
            }

            return returnvalue;
        }
    }

    public JSONObject doDescribe(String module) {
        this.checkLogin();
        Map getdata = new HashMap();
        getdata.put("operation", "describe");
        getdata.put("sessionName", this._sessionid);
        getdata.put("elementType", module);
        Object response = this._client.doGet(getdata, true);
        if (this.hasError(response)) {
            return null;
        } else {
            JSONObject result = (JSONObject) ((JSONObject) response).get("result");
            return result;
        }
    }

    public JSONObject doRetrieve(Object record) {
        this.checkLogin();
        Map getdata = new HashMap();
        getdata.put("operation", "retrieve");
        getdata.put("sessionName", this._sessionid);
        getdata.put("id", record);
        Object response = this._client.doGet(getdata, true);
        if (this.hasError(response)) {
            return null;
        } else {
            JSONObject result = (JSONObject) ((JSONObject) response).get("result");
            return result;
        }
    }

    public JSONObject doCreate(String module, Map valueMap) {
        this.checkLogin();
        if (!valueMap.containsKey("assigned_user_id")) {
            valueMap.put("assigned_user_id", this._userid);
        }

        Map postdata = new HashMap();
        postdata.put("operation", "create");
        postdata.put("sessionName", this._sessionid);
        postdata.put("elementType", module);
        postdata.put("element", this.toJSONString(valueMap));
        Object response = this._client.doPost(postdata, true);
        if (this.hasError(response)) {
            return null;
        } else {
            JSONObject result = (JSONObject) ((JSONObject) response).get("result");
            return result;
        }
    }

    public Object doInvoke(String method, Object params) {
        return this.doInvoke(method, params, "GET");
    }

    public Object doInvoke(String method, Object params, String type) {
        this.checkLogin();
        Map senddata = new HashMap();
        senddata.put("operation", method);
        senddata.put("sessionName", this._sessionid);
        Map valueMap;
        if (params != null) {
            valueMap = (Map) params;
            if (!valueMap.isEmpty()) {
                Iterator iterator = valueMap.keySet().iterator();

                while (iterator.hasNext()) {
                    Object key = iterator.next();
                    if (!senddata.containsKey(key)) {
                        senddata.put(key, valueMap.get(key));
                    }
                }
            }
        }

        valueMap = null;
        Object response;
        if (type.toUpperCase() == "POST") {
            response = this._client.doPost(senddata, true);
        } else {
            response = this._client.doGet(senddata, true);
        }
        //System.out.println(response);
        if (this.hasError(response)) {
            System.out.println("Error = " + response);
            return null;
        } else {
            Object result = ((JSONObject) response).get("result");
            return result;
        }
    }

    public Object getUserID() {
        return _userid;
    }

    public Object getLastError() {
        return _lasterror;
    }
}

