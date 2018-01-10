package data.streaming.threads.jobs;

import data.streaming.utils.Utils;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class StatsJob implements org.quartz.Job {

    public StatsJob() {
    }

    public void execute(JobExecutionContext context) throws JobExecutionException {
        Utils.calculateStats();
        System.out.println("Execution completed. Next will take part on " + context.getTrigger().getNextFireTime());
    }
}