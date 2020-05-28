import producer.RESTAPIProducer;

import java.util.Timer;
import java.util.TimerTask;

public class RESTAPIProducerExe {

    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    new RESTAPIProducer().init();
                } catch (Exception e) {
                    System.out.println(e);
                    e.printStackTrace();
                }
            }
        }, 0, 43200000);

    }
}
