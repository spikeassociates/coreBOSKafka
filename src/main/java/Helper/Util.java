package Helper;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Util {

    public static String getJson(Object object) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        String jsonInString = "";
        try {
            jsonInString = mapper.writeValueAsString(object);
            mapper.writeValue(new File("C:\\Users\\User\\Desktop\\corebos\\target\\jsonFile.json"), object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jsonInString;
    }

    public static <T> T getObjectFromJson(String jsonString, Class<T> valueType) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(jsonString, valueType);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String getProperty(String key) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(System.getProperty("user.dir") + "\\src\\main\\resources\\application.properties"));
            return properties.getProperty(key);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
