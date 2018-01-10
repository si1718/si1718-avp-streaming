package data.streaming.utils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import data.streaming.dto.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.com.google.common.collect.Maps;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.grouplens.lenskit.ItemRecommender;
import org.grouplens.lenskit.ItemScorer;
import org.grouplens.lenskit.Recommender;
import org.grouplens.lenskit.RecommenderBuildException;
import org.grouplens.lenskit.core.LenskitConfiguration;
import org.grouplens.lenskit.core.LenskitRecommender;
import org.grouplens.lenskit.data.dao.EventCollectionDAO;
import org.grouplens.lenskit.data.dao.EventDAO;
import org.grouplens.lenskit.data.event.Event;
import org.grouplens.lenskit.data.event.MutableRating;
import org.grouplens.lenskit.knn.user.UserUserItemScorer;
import org.grouplens.lenskit.scored.ScoredId;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class Utils {


    // public static String[] TAGNAMES; // = { "#OTDirecto8D", "#InmaculadaConcepcion", "Algorithm" };
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final int MAX_RECOMMENDATIONS = 5;
    private static final String MONGO_URI = "mongodb://user:user@ds119736.mlab.com:19736/si1718-avp-dissertations";
    private static final String ELSEVIER_API_KEY = "e49da12c5e54194fb30938cbf5c3fe44";
    private static final String ELSEVIER_ENDPOINT = "https://api.elsevier.com/content/search/scopus";

    public static TweetDTO createTweetDTO(String x) {
        TweetDTO result = null;

        try {
            result = mapper.readValue(x, TweetDTO.class);
        } catch (IOException e) {

        }
        return result;
    }

    public static Boolean isValid(String x) {
        Boolean result = true;

        try {
            mapper.readValue(x, TweetDTO.class);
        } catch (IOException e) {
            result = false;
        }
        return result;
    }

    public static MongoClient connectToMongo(String URI) {
        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        MongoClientURI connectionString = new MongoClientURI(URI, MongoClientOptions.builder().codecRegistry(pojoCodecRegistry));
        MongoClient mongoClient = new MongoClient(connectionString);

        return mongoClient;
    }

    public static void mostFrequentKeywordsStats() {
        MongoClient client = connectToMongo(MONGO_URI);
        MongoCollection<Document> keywordsCol = client.getDatabase("si1718-avp-dissertations").getCollection("mostFrequentKeywords");

        MongoCursor<Document> iter = getKeywordsInDatabase(client, 20);

        List<Document> toAdd = new ArrayList<>();

        while (iter.hasNext()) {
            Document next = iter.next();
            Document doc = new Document("keyword", next.get("_id"));
            doc.put("count", next.get("count"));
            toAdd.add(doc);
        }

        keywordsCol.deleteMany(new BasicDBObject());
        keywordsCol.insertMany(toAdd);
        client.close();
    }

    public static String[] getKeywords() {
        String[] result = new String[0];

        MongoClient mongoClient = connectToMongo(MONGO_URI);

        MongoCollection<Document> keywordsCol = mongoClient.getDatabase("si1718-avp-dissertations").getCollection("keywords2");

        MongoCursor<String> keywordsCursor =
                keywordsCol
                        .distinct("keyword", String.class)
                        .iterator();

        List<String> keywords = new ArrayList<>();
        while (keywordsCursor.hasNext()) {
            String thisKeyword = keywordsCursor.next();
            if (!thisKeyword.equals(""))
                keywords.add(thisKeyword);
        }

        result = keywords.toArray(result);

        mongoClient.close();

        return result;
    }

    public static void calculateStats() {
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Madrid"));
        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-d'T'HH:mm:ss.SSS'Z'");
        //sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        //Date aMinuteAgo = new Date(System.currentTimeMillis() - (60 * 1000 * 9000));
        //String strDate = sdf.format(aMinuteAgo);

        Calendar yesterday = Calendar.getInstance();
        //yesterday.setTimeZone(TimeZone.getTimeZone("Europe/Madrid"));
        yesterday.add(Calendar.DATE, -1);
        //yesterday.set(Calendar.HOUR_OF_DAY, 3);
        //yesterday.set(Calendar.MINUTE, 0);
        //yesterday.set(Calendar.SECOND, 0);

        MongoClientURI connectionString = new MongoClientURI("mongodb://user:user@ds119736.mlab.com:19736/si1718-avp-dissertations");
        MongoClient mongoClient = new MongoClient(connectionString);

        MongoCollection<Document> tweetsCol = mongoClient.getDatabase("si1718-avp-dissertations").getCollection("tweetsStats");
        List<Document> statsDocuments = new ArrayList<>();

        for (String keyword : getKeywords()) {
            List<BasicDBObject> dbObjects = new ArrayList<>();
            BasicDBObject filterFields = new BasicDBObject();

            filterFields.put("keyword", keyword);

            filterFields.put("date.day", yesterday.get(Calendar.DAY_OF_MONTH));
            filterFields.put("date.month", yesterday.get(Calendar.MONTH) + 1);
            filterFields.put("date.year", yesterday.get(Calendar.YEAR));

            BasicDBObject filter = new BasicDBObject("$match", filterFields);

            BasicDBObject groupFields = new BasicDBObject("_id", "$keyword");
            groupFields.put("total", new BasicDBObject("$sum", "$count"));

            BasicDBObject group = new BasicDBObject("$group", groupFields);

            dbObjects.add(filter);
            dbObjects.add(group);

            MongoCursor<Document> agg = tweetsCol.aggregate(dbObjects).iterator();

            Date yesterdayDate = yesterday.getTime();

            Document stat = new Document();
            stat.put("keyword", keyword);
            stat.put("date", yesterday.get(Calendar.DAY_OF_MONTH) + "/" + yesterday.get(Calendar.MONTH) + 1 + "/" + yesterday.get(Calendar.YEAR));
            stat.put("total", 0);

            // it's expected to return just a document
            if (agg.hasNext()) {
                Document aggresult = agg.next();
                stat.put("total", aggresult.get("total"));
            }

            statsDocuments.add(stat);
        }

        for (Document d : statsDocuments) {
            System.out.println(d);
        }

        MongoCollection<Document> twitterStats = mongoClient.getDatabase("si1718-avp-dissertations").getCollection("stats");
        twitterStats.insertMany(statsDocuments);

        mongoClient.close();
    }

    public static void generateRatings() {
        MongoClient mongoClient = connectToMongo(MONGO_URI);
        MongoCollection<Document> dissertationsCol = mongoClient.getDatabase("si1718-avp-dissertations").getCollection("dissertations");

        BasicDBObject projection = new BasicDBObject();
        projection.put("idDissertation", 1);
        projection.put("keywords", 1);
        projection.put("_id", 0);

        MongoCursor<DissertationKeywordDTO> dissertationsCursor = dissertationsCol.find(DissertationKeywordDTO.class).projection(projection).iterator();
        List<DissertationKeywordDTO> dissertations = new ArrayList<>();
        List<KeywordDTO> ratings = new ArrayList<>();

        while (dissertationsCursor.hasNext())
            dissertations.add(dissertationsCursor.next());

        for (DissertationKeywordDTO i : dissertations) {
            for (DissertationKeywordDTO j : dissertations) {
                if (!i.getIdDissertation().equals(j.getIdDissertation())) {
                    KeywordDTO aux = new KeywordDTO(i.getIdDissertation(), j.getIdDissertation(), 0.0);
                    if (!(CollectionUtils.isEmpty(i.getKeywords()) || CollectionUtils.isEmpty(j.getKeywords()))) {
                        List<String> intersection = new ArrayList<>();
                        intersection.addAll(i.getKeywords());

                        intersection.retainAll(j.getKeywords());
                        Integer intersectionSize = intersection.size();
                        Integer totalSize = i.getKeywords().size() + j.getKeywords().size();

                        // 2*5*number of keywords in common / sum keywords lists size
                        aux.setStatistic((2.0 * 5.0 * intersectionSize) / totalSize);
                        if (aux.getStatistic() >= 1.0)
                            ratings.add(aux);
                    }
                }
            }
        }

        MongoCollection<KeywordDTO> ratingsCol = mongoClient.getDatabase("si1718-avp-dissertations").getCollection("ratings", KeywordDTO.class);
        ratingsCol.deleteMany(new BasicDBObject());
        ratingsCol.insertMany(ratings);
        mongoClient.close();
    }

    public static Set<KeywordDTO> getRatings() throws IOException {
        MongoClient mongoClient = connectToMongo(MONGO_URI);

        MongoCursor<KeywordDTO> ratingsIter = mongoClient.getDatabase("si1718-avp-dissertations").getCollection("ratings", KeywordDTO.class).find(new BsonDocument()).iterator();

        Set<KeywordDTO> result = new HashSet<>();

        while (ratingsIter.hasNext())
            result.add(ratingsIter.next());

        mongoClient.close();
        return result;
    }

    public static ItemRecommender getRecommender(Set<KeywordDTO> dtos) throws RecommenderBuildException {
        LenskitConfiguration config = new LenskitConfiguration();
        EventDAO myDAO = EventCollectionDAO.create(createEventCollection(dtos));

        config.bind(EventDAO.class).to(myDAO);
        config.bind(ItemScorer.class).to(UserUserItemScorer.class);

        Recommender rec = LenskitRecommender.build(config);
        return rec.getItemRecommender();
    }

    private static Collection<? extends Event> createEventCollection(Set<KeywordDTO> ratings) {
        List<Event> result = new LinkedList<>();

        for (KeywordDTO dto : ratings) {
            MutableRating r = new MutableRating();
            r.setUserId(dto.getKey1().hashCode());
            r.setItemId(dto.getKey2().hashCode());
            r.setRating(dto.getStatistic());
            result.add(r);
        }
        return result;
    }

    public static void saveRecommendations(ItemRecommender irec, Set<KeywordDTO> set) throws IOException {
        List<RecommendationsDTO> recommendationsDTOList = new ArrayList<>();
        Map<String, Long> keys = Maps.asMap(set.stream().map((KeywordDTO x) -> x.getKey1()).collect(Collectors.toSet()),
                (String y) -> new Long(y.hashCode()));
        Map<Long, List<String>> reverse = set.stream().map((KeywordDTO x) -> x.getKey1())
                .collect(Collectors.groupingBy((String x) -> new Long(x.hashCode())));

        for (String key : keys.keySet()) {
            List<String> recommendations = irec.recommend(keys.get(key), MAX_RECOMMENDATIONS)
                    .stream()
                    .map(x -> reverse.get(x.getId()).get(0))
                    .filter(x -> !x.equals(key))
                    .collect(Collectors.toList());
            if (recommendations.size() > 0) {
                RecommendationsDTO recommendationsDTO = new RecommendationsDTO(key, recommendations);
                recommendationsDTOList.add(recommendationsDTO);
            }
        }

        MongoClient mongoClient = connectToMongo(MONGO_URI);

        MongoCollection<RecommendationsDTO> recommendationsCol = mongoClient.getDatabase("si1718-avp-dissertations").getCollection("recommendations", RecommendationsDTO.class);
        recommendationsCol.deleteMany(new BasicDBObject());
        recommendationsCol.insertMany(recommendationsDTOList);

        mongoClient.close();
    }

    public static void dissertationsPerYearStats() {
        MongoClient client = connectToMongo(MONGO_URI);
        MongoCollection<Document> dissertationsCol = client.getDatabase("si1718-avp-dissertations").getCollection("dissertations");
        MongoCollection<Document> yearStatsCol = client.getDatabase("si1718-avp-dissertations").getCollection("dissertationsPerYear");

        List<BasicDBObject> aggBson = new ArrayList<>();

        BasicDBObject projectFields = new BasicDBObject();
        projectFields.put("_id", 0);
        projectFields.put("year", 1);

        BasicDBObject filterBounds = new BasicDBObject("$gte", 1990);
        filterBounds.put("$lte", 2020);
        BasicDBObject filter = new BasicDBObject("year", filterBounds);
        BasicDBObject match = new BasicDBObject("$match", filter);

        BasicDBObject project = new BasicDBObject("$project", projectFields);

        BasicDBObject groupFields = new BasicDBObject("_id", "$year");
        groupFields.put("count", new BasicDBObject("$sum", 1));

        BasicDBObject group = new BasicDBObject("$group", groupFields);

        BasicDBObject sort = new BasicDBObject("$sort", new BasicDBObject("year", -1));

        aggBson.add(project);
        aggBson.add(match);
        aggBson.add(group);
        aggBson.add(sort);

        MongoCursor<Document> iter = dissertationsCol.aggregate(aggBson).iterator();
        List<Document> toAdd = new ArrayList<>();

        while (iter.hasNext()) {
            Document next = iter.next();
            Document doc = new Document("year", next.get("_id"));
            doc.put("count", next.get("count"));
            toAdd.add(doc);
        }

        yearStatsCol.deleteMany(new BasicDBObject());
        yearStatsCol.insertMany(toAdd);
        client.close();

    }

    public static void dissertationsPerTutorStats() {
        MongoClient client = connectToMongo(MONGO_URI);
        MongoCollection<Document> dissertationsCol = client.getDatabase("si1718-avp-dissertations").getCollection("dissertations");
        MongoCollection<Document> tutorsStatsCol = client.getDatabase("si1718-avp-dissertations").getCollection("dissertationsPerTutor");

        List<BasicDBObject> aggBson = new ArrayList<>();

        BasicDBObject unwind = new BasicDBObject("$unwind", "$tutors");

        BasicDBObject projectFields = new BasicDBObject();
        projectFields.put("_id", 0);
        projectFields.put("tutors.name", 1);

        BasicDBObject project = new BasicDBObject("$project", projectFields);

        BasicDBObject groupFields = new BasicDBObject("_id", "$tutors.name");
        groupFields.put("count", new BasicDBObject("$sum", 1));

        BasicDBObject group = new BasicDBObject("$group", groupFields);

        BasicDBObject sort = new BasicDBObject("$sort", new BasicDBObject("count", -1));

        BasicDBObject limit = new BasicDBObject("$limit", 20);

        aggBson.add(unwind);
        aggBson.add(project);
        aggBson.add(group);
        aggBson.add(sort);
        aggBson.add(limit);

        MongoCursor<Document> iter = dissertationsCol.aggregate(aggBson).iterator();
        List<Document> toAdd = new ArrayList<>();

        while (iter.hasNext()) {
            Document next = iter.next();
            Document doc = new Document("tutor", next.get("_id"));
            doc.put("count", next.get("count"));
            toAdd.add(doc);
        }

        tutorsStatsCol.deleteMany(new BasicDBObject());
        tutorsStatsCol.insertMany(toAdd);
        client.close();

    }

    public static void tagsInElsevierStats() {
        MongoClient client = connectToMongo(MONGO_URI);
        MongoCursor<Document> keywords = getKeywordsInDatabase(client, 500);
        List<Document> toSave = new ArrayList<>();

        Document nextDocument;
        while (keywords.hasNext()) {
            nextDocument = keywords.next();
            String keyword = nextDocument.get("_id").toString();
            Document toAdd = new Document("keyword", keyword);

            JSONObject results = null;
            try {
                String resultsStr = getHttpResults(ELSEVIER_ENDPOINT + "?query=EXACTSRCTITLE("+URLEncoder.encode(keyword, "UTF-8")+")&apiKey=" + ELSEVIER_API_KEY + "&httpAccept=application/json");
                results = new JSONObject(resultsStr);
            } catch (UnsupportedEncodingException e) {

            } catch (JSONException e) {
                results = null;
            }
            if (results != null) {
                try {
                    Integer count = new Integer(results.getJSONObject("search-results").getString("opensearch:totalResults"));
                    toAdd.put("count", count);
                } catch (Exception e) {
                    toAdd.put("count", 0);
                }
            } else {
                toAdd.put("count", 0);
            }
            System.out.println(toAdd);
            toSave.add(toAdd);
        }

        MongoCollection<Document> mostFrequentElsevier = client.getDatabase("si1718-avp-dissertations").getCollection("mostFrequentKeywordsElsevier");
        mostFrequentElsevier.deleteMany(new BasicDBObject());
        mostFrequentElsevier.insertMany(toSave);
    }

    public static void dissertationPerGroupStats(){
        MongoClient client = connectToMongo(MONGO_URI);
        MongoCollection<Document> dissertationsCol = client.getDatabase("si1718-avp-dissertations").getCollection("dissertations");
        MongoCollection<Document> groupsCol = client.getDatabase("si1718-avp-dissertations").getCollection("dissertationsPerGroup");

        List<BasicDBObject> aggBson = new ArrayList<>();

        BasicDBObject unwind = new BasicDBObject("$unwind", "$tutors");

        BasicDBObject projectFields = new BasicDBObject();
        projectFields.put("_id", 0);
        projectFields.put("tutors.name", 1);

        BasicDBObject project = new BasicDBObject("$project", projectFields);

        aggBson.add(unwind);
        aggBson.add(project);

        MongoCursor<Document> iter = dissertationsCol.aggregate(aggBson).iterator();
        List<String> tutors = new ArrayList<>();

        while (iter.hasNext()) {
            Document next = iter.next();
            Document sub = (Document) next.get("tutors");
            String nextStr = sub.get("name").toString();
            System.out.println(sub);
            tutors.add(nextStr);
        }

        List<GroupDTO> groupDTOS;
        try {
            String groupsStr = getHttpResults("https://si1718-rgg-groups.herokuapp.com/api/v1/groups");
            JSONArray groups = null;
            groups = new JSONArray(groupsStr);

            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            String jsonArrStr = groups.toString();
            groupDTOS=mapper.readValue(jsonArrStr, new TypeReference<List<GroupDTO>>(){});
        } catch (IOException e) {
            groupDTOS = new ArrayList<>();
            e.printStackTrace();
        } catch (JSONException e) {
            groupDTOS = new ArrayList<>();
            e.printStackTrace();
        }

        List<GroupDTO> finalList = groupDTOS;
        System.out.println(groupDTOS);

        Map<String, Long> countByGroup = tutors.stream().map(x -> {
            return finalList.stream()
                    .filter(y ->
                                StringUtils.stripAccents(y.getLeader()).equals(StringUtils.stripAccents(x)) ||
                                        y.getComponents().stream()
                                                .map(z -> StringUtils.stripAccents(z))
                                                .collect(Collectors.toList()).contains(StringUtils.stripAccents(x))
                    ).map(y -> y.getName()).findFirst().orElse("no-group");
        })
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        System.out.println(countByGroup);

        List<Document> toSave = new ArrayList<>();

        for(String k: countByGroup.keySet()){
            Document d = new Document("group", k);
            d.put("count", countByGroup.get(k));
            toSave.add(d);
        }

        groupsCol.deleteMany(new BasicDBObject());
        groupsCol.insertMany(toSave);
    }

    private static String getHttpResults(String URI) {
        String result;
        try {
            String url = URI;

            URL obj = new URL(url);

            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");

            String response = IOUtils.toString(con.getInputStream());

            result = response;

        } catch (IOException e) {
            result = null;
            e.printStackTrace();
        }
        return result;
    }

    private static MongoCursor<Document> getKeywordsInDatabase(MongoClient client, int limit) {
        MongoCollection<Document> dissertationsCol = client.getDatabase("si1718-avp-dissertations").getCollection("dissertations");

        List<BasicDBObject> aggBson = new ArrayList<>();

        BasicDBObject projectFields = new BasicDBObject();
        projectFields.put("_id", 0);
        projectFields.put("keywords", 1);

        BasicDBObject project = new BasicDBObject("$project", projectFields);

        BasicDBObject unwind = new BasicDBObject("$unwind", "$keywords");

        BasicDBObject groupFields = new BasicDBObject("_id", "$keywords");
        groupFields.put("count", new BasicDBObject("$sum", 1));

        BasicDBObject group = new BasicDBObject("$group", groupFields);

        BasicDBObject sort = new BasicDBObject("$sort", new BasicDBObject("count", -1));

        aggBson.add(project);
        aggBson.add(unwind);
        aggBson.add(group);
        aggBson.add(sort);

        if (limit > 0) {
            aggBson.add(new BasicDBObject("$limit", limit));
        }


        return dissertationsCol.aggregate(aggBson).iterator();
    }

    public static Calendar getDateFromTweet(TweetDTO tweetDTO) {
        SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z y");
        Calendar createdAtCal = Calendar.getInstance();
        try {
            createdAtCal.setTime(sdf.parse(tweetDTO.getCreatedAt()));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return createdAtCal;

    }

    /**
     * <p>Checks if two calendars represent the same day ignoring time.</p>
     *
     * @param cal1 the first calendar, not altered, not null
     * @param cal2 the second calendar, not altered, not null
     * @return true if they represent the same day
     * @throws IllegalArgumentException if either calendar is <code>null</code>
     */
    public static boolean isSameDay(Calendar cal1, Calendar cal2) {
        if (cal1 == null || cal2 == null) {
            throw new IllegalArgumentException("The dates must not be null");
        }
        return (cal1.get(Calendar.ERA) == cal2.get(Calendar.ERA) &&
                cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR) &&
                cal1.get(Calendar.DAY_OF_YEAR) == cal2.get(Calendar.DAY_OF_YEAR));
    }
}
