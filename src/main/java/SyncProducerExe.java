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
                new SyncProducer().init();
            }
        }, 0, 1000 * 60 * SyncProducer.timeIntervalMin);

    }
}
