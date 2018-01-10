package data.streaming.threads;

import data.streaming.threads.jobs.*;
import data.streaming.utils.Utils;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Date;
import java.util.concurrent.*;

public class Batch {

    public static TimeOfDay RATINGS_JOB_TIME = TimeOfDay.hourAndMinuteOfDay(3, 0);
    public static TimeOfDay RECOMMENDATIONS_JOB_TIME = TimeOfDay.hourAndMinuteOfDay(4, 0);
    public static TimeOfDay SCRAPING_JOB_TIME = TimeOfDay.hourAndMinuteOfDay(2, 0);
    public static TimeOfDay STATS_JOB_TIME = TimeOfDay.hourAndMinuteOfDay(1, 05);
    public static TimeOfDay DISSERTATIONS_PER_YEAR_STATS_JOB_TIME = TimeOfDay.hourAndMinuteOfDay(1, 15);
    public static TimeOfDay DISSERTATIONS_PER_TUTOR_STATS_JOB_TIME = TimeOfDay.hourAndMinuteOfDay(1, 20);
    public static TimeOfDay MOST_FREQUENT_KEYWORDS_STATS_JOB_TIME = TimeOfDay.hourAndMinuteOfDay(1, 30);
    public static TimeOfDay TAGS_IN_ELSEVIER_STATS_JOB_TIME = TimeOfDay.hourAndMinuteOfDay(4, 30);
    public static TimeOfDay DISSERTATIONS_PER_GROUP_STATS_JOB_TIME = TimeOfDay.hourAndMinuteOfDay(6, 0);

    public static void main(String... args) throws SchedulerException {
        System.out.println(new Date());

        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

        // ratings job
        JobDetail ratingsJob = JobBuilder.newJob(RatingsJob.class)
                .withIdentity("RatingsJob", "Recommender System")
                .build();

        Trigger ratingsJobTrigger = TriggerBuilder.newTrigger()
                .withIdentity("RatingsJob Trigger", "Recommender System")
                .withSchedule(
                        DailyTimeIntervalScheduleBuilder
                                .dailyTimeIntervalSchedule()
                                .onEveryDay()
                                .withIntervalInHours(24)
                                .startingDailyAt(RATINGS_JOB_TIME)
                )
                .build();

        // recommendations job
        JobDetail recommendationsJob = JobBuilder.newJob(RecommendationsJob.class)
                .withIdentity("RecommendationsJob", "Recommender System")
                .build();

        Trigger recommendationsJobTrigger = TriggerBuilder.newTrigger()
                .withIdentity("RecommendationsJob Trigger", "Recommender System")
                .withSchedule(
                        DailyTimeIntervalScheduleBuilder
                                .dailyTimeIntervalSchedule()
                                .onEveryDay()
                                .withIntervalInHours(24)
                                .startingDailyAt(RECOMMENDATIONS_JOB_TIME)
                )
                .build();


        // scrap dissertations job
        JobDetail scrapingJob = JobBuilder.newJob(ScrapingJob.class)
                .withIdentity("ScrapingJob", "Web Scraping")
                .build();

        Trigger scrapingJobTrigger = TriggerBuilder.newTrigger()
                .withIdentity("ScrapingJob Trigger", "Web Scraping")
                .withSchedule(
                        DailyTimeIntervalScheduleBuilder
                                .dailyTimeIntervalSchedule()
                                .onEveryDay()
                                .withIntervalInHours(24)
                                .startingDailyAt(SCRAPING_JOB_TIME)
                )
                .build();

        // stats job
        JobDetail statsJob = JobBuilder.newJob(StatsJob.class)
                .withIdentity("StatsJob", "Stats")
                .build();

        Trigger statsJobTrigger = TriggerBuilder.newTrigger()
                .withIdentity("StatsJob Trigger", "Stats")
                .withSchedule(
                        DailyTimeIntervalScheduleBuilder
                                .dailyTimeIntervalSchedule()
                                .onEveryDay()
                                .withIntervalInHours(24)
                                .startingDailyAt(STATS_JOB_TIME)
                )
                .build();

        // dissertations per year stats
        JobDetail dissertationsPerYearJob = JobBuilder.newJob(DissertationsPerYearStatsJob.class)
                .withIdentity("DissertationsPerYearJob", "Stats")
                .build();

        Trigger dissertationsPerYearJobTrigger = TriggerBuilder.newTrigger()
                .withIdentity("DissertationsPerYearJob Trigger", "Stats")
                .withSchedule(
                        DailyTimeIntervalScheduleBuilder
                                .dailyTimeIntervalSchedule()
                                .onEveryDay()
                                .withIntervalInHours(24)
                                .startingDailyAt(DISSERTATIONS_PER_YEAR_STATS_JOB_TIME)
                )
                .build();

        // dissertations per tutor stats
        JobDetail dissertationsPerTutorJob = JobBuilder.newJob(DissertationsPerTutorStatsJob.class)
                .withIdentity("DissertationsPerTutorJob", "Stats")
                .build();

        Trigger dissertationsPerTutorJobTrigger = TriggerBuilder.newTrigger()
                .withIdentity("DissertationsPerTutorJob Trigger", "Stats")
                .withSchedule(
                        DailyTimeIntervalScheduleBuilder
                                .dailyTimeIntervalSchedule()
                                .onEveryDay()
                                .withIntervalInHours(24)
                                .startingDailyAt(DISSERTATIONS_PER_TUTOR_STATS_JOB_TIME)
                )
                .build();

        // most frequent keywords stats
        JobDetail mostFrequentKeywordsJob = JobBuilder.newJob(MostFrequentKeywordsStatsJob.class)
                .withIdentity("MostFrequentKeywordsJob", "Stats")
                .build();

        Trigger mostFrequentKeywordsJobTrigger = TriggerBuilder.newTrigger()
                .withIdentity("MostFrequentKeywordsJob Trigger", "Stats")
                .withSchedule(
                        DailyTimeIntervalScheduleBuilder
                                .dailyTimeIntervalSchedule()
                                .onEveryDay()
                                .withIntervalInHours(24)
                                .startingDailyAt(MOST_FREQUENT_KEYWORDS_STATS_JOB_TIME)
                )
                .build();

        // elsevier ststs
        JobDetail tagsInElsevierJob = JobBuilder.newJob(TagsInElsevierStatsJob.class)
                .withIdentity("TagsInElsevierJob", "Stats")
                .build();

        Trigger tagsInElsevierJobTrigger = TriggerBuilder.newTrigger()
                .withIdentity("TagsInElsevierJob Trigger", "Stats")
                .withSchedule(
                        DailyTimeIntervalScheduleBuilder
                                .dailyTimeIntervalSchedule()
                                .onEveryDay()
                                .withIntervalInHours(24)
                                .startingDailyAt(TAGS_IN_ELSEVIER_STATS_JOB_TIME)
                )
                .build();

        // dissertations per group
        JobDetail dissertatiosPerGroupStatsJob = JobBuilder.newJob(DissertationsPerGroupStats.class)
                .withIdentity("DissertatiosPerGroupStatsJob", "Stats")
                .build();

        Trigger dissertatiosPerGroupStatsJobTrigger = TriggerBuilder.newTrigger()
                .withIdentity("DissertatiosPerGroupStatsJob Trigger", "Stats")
                .withSchedule(
                        DailyTimeIntervalScheduleBuilder
                                .dailyTimeIntervalSchedule()
                                .onEveryDay()
                                .withIntervalInHours(24)
                                .startingDailyAt(DISSERTATIONS_PER_GROUP_STATS_JOB_TIME)
                )
                .build();

        scheduler.scheduleJob(ratingsJob, ratingsJobTrigger);
        scheduler.scheduleJob(recommendationsJob, recommendationsJobTrigger);
        scheduler.scheduleJob(scrapingJob, scrapingJobTrigger);
        scheduler.scheduleJob(statsJob, statsJobTrigger);
        scheduler.scheduleJob(dissertationsPerYearJob, dissertationsPerYearJobTrigger);
        scheduler.scheduleJob(dissertationsPerTutorJob, dissertationsPerTutorJobTrigger);
        scheduler.scheduleJob(mostFrequentKeywordsJob, mostFrequentKeywordsJobTrigger);
        scheduler.scheduleJob(tagsInElsevierJob, tagsInElsevierJobTrigger);
        scheduler.scheduleJob(dissertatiosPerGroupStatsJob, dissertatiosPerGroupStatsJobTrigger);
        scheduler.start();
        System.out.println("Scheduling completed.");

    }

}
