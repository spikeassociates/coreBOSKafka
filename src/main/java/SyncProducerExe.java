import producer.SyncProducer;

import java.util.Timer;
import java.util.TimerTask;

public class SyncProducerExe {

    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() { // Function runs every SimpleProducer.timeIntervalMin minutes.
                // Run the code you want here
                try {
                    new SyncProducer().init();
                } catch (Exception e) {
                    System.out.println(e);
                    e.printStackTrace();
                }
            }
        }, 0, 1000 * 60 * SyncProducer.timeIntervalMin);

    }
}
