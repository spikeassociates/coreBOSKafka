package helper;

import java.io.File;

public class Config {

    private String lastTimeStampSync = "";


    private static Config config;

    public static synchronized Config getInstance() {
        if (config != null)
            return config;

        String pathConfigFile = Util.coreBossJsonDir + Util.coreBossConfigJsonFile + ".json";
        File configFile = new File(pathConfigFile);

        if (configFile.exists())
            config = Util.getObjectFromJson(configFile, Config.class);
        else
            config = new Config();

        return config;
    }


    public synchronized void save() {
        Util.createJSonFile(config, Util.coreBossConfigJsonFile);
    }

    public String getLastTimeStampSync() {
        return lastTimeStampSync;
    }

    public void setLastTimeStampSync(String lastTimeStampSync) {
        this.lastTimeStampSync = lastTimeStampSync;
        save();
    }
}
