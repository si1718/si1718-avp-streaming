package data.streaming.threads.jobs;

import data.streaming.dto.KeywordDTO;
import data.streaming.utils.Utils;
import org.grouplens.lenskit.ItemRecommender;
import org.grouplens.lenskit.RecommenderBuildException;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.IOException;
import java.util.Set;

public class RecommendationsJob implements org.quartz.Job {

    public RecommendationsJob() {
    }

    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            Set<KeywordDTO> set = Utils.getRatings();
            ItemRecommender irec = Utils.getRecommender(set);
            Utils.saveRecommendations(irec, set);
        } catch (IOException e) {
            e.printStackTrace();
        }catch (RecommenderBuildException e) {
            e.printStackTrace();
        }
        System.out.println("Execution completed. Next will take part on " + context.getTrigger().getNextFireTime());
    }
}