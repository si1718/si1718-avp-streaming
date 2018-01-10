package data.streaming.utils;

import data.streaming.dto.KeywordDTO;
import data.streaming.scraping.ScrapDissertations;
import org.grouplens.lenskit.ItemRecommender;
import org.grouplens.lenskit.RecommenderBuildException;

import java.io.IOException;
import java.util.Set;

public class MainTest {
    public static void main(String[] args) throws IOException {
        Utils.dissertationPerGroupStats();
    }
}
