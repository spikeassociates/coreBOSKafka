package model;

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

}
