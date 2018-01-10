package data.streaming.threads.jobs;

import data.streaming.dto.KeywordDTO;
import data.streaming.scraping.ScrapDissertations;
import data.streaming.utils.Utils;
import org.grouplens.lenskit.ItemRecommender;
import org.grouplens.lenskit.RecommenderBuildException;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.IOException;
import java.util.Set;

public class ScrapingJob implements org.quartz.Job {

    public ScrapingJob() {
    }

    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            ScrapDissertations.execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Execution completed. Next will take part on " + context.getTrigger().getNextFireTime());
    }
}