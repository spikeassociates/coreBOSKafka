//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package vtwslib;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HTTP_Client extends DefaultHttpClient {
    private String _serviceurl;

    public HTTP_Client(String url) {
        this._serviceurl = url;
    }

    protected void finalize() throws Throwable {
        this.getConnectionManager().shutdown();
        super.finalize();
    }

    public Object doGet(Object data) {
        return this.doGet(data, false);
    }

    public Object doGet(Object data, boolean convertToJSON) {
        try {
            String uri = this._serviceurl;
            if (data != null) {
                if (!uri.endsWith("?")) {
                    uri = uri + "?";
                }

                if (data instanceof String) {
                    uri = uri + data;
                } else if (data instanceof Map) {
                    List params = new ArrayList();
                    Map dataMap = (Map) data;
                    Iterator iterator = dataMap.keySet().iterator();

                    while (iterator.hasNext()) {
                        Object key = iterator.next();
                        params.add(new BasicNameValuePair(key.toString(), (String) dataMap.get(key)));
                    }

                    uri = uri + URLEncodedUtils.format(params, "UTF-8");
                }
            }

            HttpGet httpGet = new HttpGet(uri);
            HttpResponse httpResponse = this.execute(httpGet);
            HttpEntity httpEntity = httpResponse.getEntity();
            String response = EntityUtils.toString(httpEntity);
            return convertToJSON ? this.__jsondecode(response) : response;
        } catch (Exception var8) {
            return var8;
        }
    }

    public Object doPost(Object data) {
        return this.doPost(data, false);
    }

    public Object doPost(Object data, boolean convertToJSON) {
        try {
            String uri = this._serviceurl;
            HttpPost httpPost = new HttpPost(uri);
            List params = new ArrayList();
            if (data instanceof Map) {
                Map dataMap = (Map) data;
                Iterator iterator = dataMap.keySet().iterator();

                while (iterator.hasNext()) {
                    Object key = iterator.next();
                    params.add(new BasicNameValuePair(key.toString(), (String) dataMap.get(key)));
                }
            }

            httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
            HttpResponse httpResponse = this.execute(httpPost);
            HttpEntity httpEntity = httpResponse.getEntity();
            String response = EntityUtils.toString(httpEntity);
            return convertToJSON ? this.__jsondecode(response) : response;
        } catch (Exception var9) {
            return var9;
        }
    }

    public Object __jsondecode(String input) {
        return JSONValue.parse(input);
    }

    public String __jsonencode(Object input) {
        return JSONValue.toJSONString(input);
    }
}
