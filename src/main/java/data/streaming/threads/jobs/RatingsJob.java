package data.streaming.threads.jobs;

import data.streaming.utils.Utils;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class RatingsJob implements org.quartz.Job {

    public RatingsJob() {
    }

    public void execute(JobExecutionContext context) throws JobExecutionException {
        Utils.generateRatings();
        System.out.println("Execution completed. Next will take part on " + context.getTrigger().getNextFireTime());
    }
}