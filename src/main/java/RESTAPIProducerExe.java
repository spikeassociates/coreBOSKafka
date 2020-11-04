import cron.QuartzJob;
import helper.Util;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

public class RESTAPIProducerExe {
    private static String cronExpression = (Util.getProperty("corebos.restproducer.cronexpression").isEmpty()) ? System.getenv("PRODUCER_CRON_EXPRESSION") : Util.getProperty("corebos.restproducer.cronexpression");

    public static void main(String[] args) throws SchedulerException {
        JobDetail jobDetail = JobBuilder.newJob(QuartzJob.class).withIdentity("FetchShipmentData", "installo").build();
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("CronTrigger", "installo").withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)).build();
        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
        scheduler.start();
        scheduler.scheduleJob(jobDetail, trigger);
    }
}
