package model;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class ValueData {
    public String SquareId;
    public String TimeInterval;
    public String CountryCode;
    public String SmsInActivity;
    public String SmsOutActivity;
    public String CallInActivity;
    public String CallOutActivity;
    public String InternetTrafficActivity;
    public User user;

    public ValueData() {

    }

    public ValueData(String SquareId) {
        this.SquareId = SquareId;
        this.TimeInterval = "testTimeInterval";
        this.CountryCode = "testCountryCode";
        this.SmsInActivity = "testSmsInActivity";
        this.SmsOutActivity = "testSmsOutActivity";
        this.CallInActivity = "testCallInActivity";
        this.CallOutActivity = "testCallOutActivity";
        this.InternetTrafficActivity = "testCallOutActivity";
        this.user = new User("testFirsname", "testLasname");
    }


    public static ValueData getObjectFromJson(String jsonString) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(jsonString, ValueData.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static ValueData getObjectFromJson(File jsonFile) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(jsonFile, ValueData.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
