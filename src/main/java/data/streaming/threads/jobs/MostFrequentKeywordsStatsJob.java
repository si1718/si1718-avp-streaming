package data.streaming.threads.jobs;

import data.streaming.utils.Utils;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class MostFrequentKeywordsStatsJob implements org.quartz.Job {

    public MostFrequentKeywordsStatsJob() {
    }

    public void execute(JobExecutionContext context) throws JobExecutionException {
        Utils.mostFrequentKeywordsStats();
        System.out.println("Execution completed. Next will take part on " + context.getTrigger().getNextFireTime());
    }
}