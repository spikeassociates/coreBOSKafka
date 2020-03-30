package siae.elastic;

import com.fasterxml.jackson.core.JsonProcessingException;
import helper.Util;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

public class Elastic {

    private static final String HOST = Util.getProperty("corebos.siae.elastic_host");
    private static final int PORT_ONE = Integer.parseInt(Util.getProperty("corebos.siae.elastic_port"));
    private static final int PORT_TWO = 9201;
    private static final String SCHEME = Util.getProperty("corebos.siae.elastic_scheme");


    private static RestHighLevelClient restHighLevelClient;

    private static final String INDEX = "ticket_access_information";
    private static final String TYPE = "_doc";


    private synchronized RestHighLevelClient makeConnection() {

        if (restHighLevelClient == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(HOST, PORT_ONE, SCHEME),
                            new HttpHost(HOST, PORT_TWO, SCHEME)));
        }

        return restHighLevelClient;
    }

    private synchronized void closeConnection() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }

    public Map insertData(Map data) {
        return insertData(INDEX, TYPE, data);
    }

    public Map insertData(String index, String type, Map data) {
        makeConnection();
        IndexRequest indexRequest = new IndexRequest(index, type, (String) data.get("id"))
                .source(data);
        IndexResponse response = null;
        try {
            response = restHighLevelClient.index(indexRequest);
            data.put("id", response.getId());
        } catch (ElasticsearchException e) {
            e.getDetailedMessage();
        } catch (java.io.IOException ex) {
            ex.getLocalizedMessage();
        }
        try {
            closeConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("response.status().toString() = " + response.status().toString());
        System.out.println("response.toString() = " + response.toString());
        return data;
    }


    public Map updateData(String id, Map data) {
        return updateData(INDEX, TYPE, id, data);
    }

    public Map updateData(String index, String type, String id, Map data) {
        makeConnection();
        UpdateRequest updateRequest = new UpdateRequest(index, type, id)
                .fetchSource(true);    // Fetch Object after its update
        UpdateResponse updateResponse = null;
        try {
            String dataJson = Util.getJson(data);
            updateRequest.doc(dataJson, XContentType.JSON);
            updateResponse = restHighLevelClient.update(updateRequest);
        } catch (JsonProcessingException e) {
            e.getMessage();
        } catch (java.io.IOException e) {
            e.getLocalizedMessage();
        }
        try {
            closeConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return updateResponse != null ? updateResponse.getGetResult().sourceAsMap() : null;
    }

    public void deleteData(String id) {
        deleteData(INDEX, TYPE, id);
    }

    public void deleteData(String index, String type, String id) {
        makeConnection();
        DeleteRequest deleteRequest = new DeleteRequest(index, type, id);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest);
        } catch (java.io.IOException e) {
            e.getLocalizedMessage();
        }
        try {
            closeConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
