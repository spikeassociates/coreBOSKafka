package cron;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import producer.RESTAPIProducer;

public class QuartzJob implements Job {
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        try {
            new RESTAPIProducer().init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
