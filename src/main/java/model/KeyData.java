package model;


import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class KeyData {
    public String SquareId;

    public static KeyData getObjectFromJson(String jsonString) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(jsonString, KeyData.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static KeyData getObjectFromJson(File jsonFile) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(jsonFile, KeyData.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
