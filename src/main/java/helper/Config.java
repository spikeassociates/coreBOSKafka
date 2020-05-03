package helper;

import java.io.File;

public class Config {

    private String lastTimeStampToSync = "";
    private String totalNumberOfPages = "";
    private String isFirstRequest = "";


    private static Config config;

    public static synchronized Config getInstance() {
        if (config != null)
            return config;

        String pathConfigFile = Util.coreBossJsonDir + Util.coreBossConfigJsonFile + ".json";
        File configFile = new File(pathConfigFile);

        if (configFile.exists())
            config = Util.getObjectFromJson(configFile, Config.class);

        if (config == null)
            config = new Config();

        return config;
    }


    public synchronized void save() {
        Util.createJSonFile(config, Util.coreBossConfigJsonFile);
    }

    public String getLastTimeStampToSync() {
        return lastTimeStampToSync;
    }

    public void setLastTimeStampToSync(String lastTimeStampSync) {
        this.lastTimeStampToSync = lastTimeStampSync;
    }

    public String getTotalNumberOfPages() {
        return totalNumberOfPages;
    }

    public void setTotalNumberOfPages(String totalNumberOfPages) {
        this.totalNumberOfPages = totalNumberOfPages;
    }

    public String isFirstRequest() {
        return isFirstRequest;
    }

    public void setFirstRequest(String firstRequest) {
        this.isFirstRequest = firstRequest;
    }
}
