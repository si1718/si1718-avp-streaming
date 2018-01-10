package data.streaming.threads;

import data.streaming.threads.jobs.FlinkKafkaProducerJob;
import data.streaming.threads.jobs.RatingsJob;
import data.streaming.threads.jobs.RecommendationsJob;
import data.streaming.threads.jobs.StatsJob;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Date;

public class Streaming {


    public static TimeOfDay RATINGS_JOB_TIME = TimeOfDay.hourAndMinuteOfDay(3, 0);
    public static TimeOfDay RECOMMENDATIONS_JOB_TIME = TimeOfDay.hourAndMinuteOfDay(6, 0);
    public static TimeOfDay SCRAPING_JOB_TIME = TimeOfDay.hourAndMinuteOfDay(1, 0);
    public static TimeOfDay STATS_JOB_TIME = TimeOfDay.hourAndMinuteOfDay(0, 10);

    public static void main(String... args) throws SchedulerException, InterruptedException {
        System.out.println(new Date());

        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

        // ratings job
        JobDetail producerJob = JobBuilder.newJob(FlinkKafkaProducerJob.class)
                .withIdentity("ProducerJob", "Streaming")
                .build();

        Trigger producerJobTrigger = TriggerBuilder.newTrigger()
                .withIdentity("ProducerJob Trigger", "Streaming")
                .startNow()
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(20).repeatForever())
                .build();

        /*
        DailyTimeIntervalScheduleBuilder
                .dailyTimeIntervalSchedule()
                .onEveryDay()
                .withIntervalInHours(24)
                .startingDailyAt(RATINGS_JOB_TIME);
*/

        scheduler.scheduleJob(producerJob, producerJobTrigger);

        scheduler.start();

        System.out.println("Scheduling completed. First execution started");
    }
}
