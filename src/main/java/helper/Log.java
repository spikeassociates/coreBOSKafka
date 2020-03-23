package helper;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class Log {
    private static Log log;
    private Logger logger = Logger.getLogger(Log.class);

    public static synchronized Logger getLogger() {
        if (log == null) {
            log = new Log();
            BasicConfigurator.configure();
        }
        return log.logger;
    }
}
