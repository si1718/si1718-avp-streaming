package data.streaming.threads.jobs;

import data.streaming.utils.Utils;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class DissertationsPerYearStatsJob implements org.quartz.Job {

    public DissertationsPerYearStatsJob() {
    }

    public void execute(JobExecutionContext context) throws JobExecutionException {
        Utils.dissertationsPerYearStats();
        System.out.println("Execution completed. Next will take part on " + context.getTrigger().getNextFireTime());
    }
}