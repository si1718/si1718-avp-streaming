package data.streaming.threads.jobs;

import data.streaming.utils.Utils;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class DissertationsPerGroupStats implements org.quartz.Job {

    public DissertationsPerGroupStats() {
    }

    public void execute(JobExecutionContext context) throws JobExecutionException {
        Utils.dissertationPerGroupStats();
        System.out.println("Execution completed. Next will take part on " + context.getTrigger().getNextFireTime());
    }
}