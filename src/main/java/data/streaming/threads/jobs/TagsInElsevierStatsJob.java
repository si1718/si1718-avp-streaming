package data.streaming.threads.jobs;

import data.streaming.utils.Utils;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class TagsInElsevierStatsJob implements org.quartz.Job {

    public TagsInElsevierStatsJob() {
    }

    public void execute(JobExecutionContext context) throws JobExecutionException {
        Utils.tagsInElsevierStats();
        System.out.println("Execution completed. Next will take part on " + context.getTrigger().getNextFireTime());
    }
}