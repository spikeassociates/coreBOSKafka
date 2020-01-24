package helper;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class Util {

    public static final String coreBossDir = System.getProperty("user.dir") + "\\corebos\\";
    public static final String coreBossJsonDir = coreBossDir + "json\\";

    public static final String coreBossConfigJsonFile = "config";

    public static final String dafaultTime = "5";


    public static final String elementTypeACCOUNTS = "Accounts";
    public static final String elementTypeCONTACTS = "Contacts";


    public static final String methodCREATE = "create";
    public static final String methodUPDATE = "update";
    public static final String methodDELETE = "delete";
    public static final String methodUPSERT = "upsert";

    public static String getJson(Object object) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        try {
            return mapper.writeValueAsString(object);
//            mapper.writeValue(new File("C:\\Users\\User\\Desktop\\corebos\\target\\jsonFile.json"), object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String createJSonFile(Object object, String... name) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        String path = coreBossJsonDir;
        if (name[0] != null) {
            path += name[0] + ".json";
        } else {
            path += "jsonFile.json";
        }
        try {
            File file = new File(path);
            Files.createDirectories(Paths.get(file.getParent()));
            mapper.writeValue(file, object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
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

    public static <T> T getObjectFromJson(File jsonFile, Class<T> valueType) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(jsonFile, valueType);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String getProperty(String key) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(coreBossDir + "application.properties"));
            return properties.getProperty(key);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
