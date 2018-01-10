package data.streaming.threads.jobs;

import data.streaming.utils.Utils;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class DissertationsPerTutorStatsJob implements org.quartz.Job {

    public DissertationsPerTutorStatsJob() {
    }

    public void execute(JobExecutionContext context) throws JobExecutionException {
        Utils.dissertationsPerTutorStats();
        System.out.println("Execution completed. Next will take part on " + context.getTrigger().getNextFireTime());
    }
}