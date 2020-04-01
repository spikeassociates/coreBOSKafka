import producer.QueryProducer;

import java.util.Timer;
import java.util.TimerTask;

public class QueryProducerExe {
    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    new QueryProducer().init();
                } catch (Exception e) {
                    System.out.println(e);
                    e.printStackTrace();
                }
            }
        }, 0, 1000 * 60 * QueryProducer.timeIntervalMin);

    }
}
