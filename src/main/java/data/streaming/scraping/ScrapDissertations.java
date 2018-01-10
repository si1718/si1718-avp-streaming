package data.streaming.scraping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.slugify.Slugify;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import data.streaming.utils.Utils;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import data.streaming.dto.Dissertation;
import data.streaming.dto.Tutor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

public class ScrapDissertations {

    public static final String BASE_URL = "http://investigacion.us.es";
    public static Map<String, String> ID_MAP = new HashMap<>();
    public static List<Dissertation> DISSERTATIONS_WITHOUT_AUTHOR_ID = new ArrayList<>();
    public static Map<String, Dissertation> DISSERTATIONS_BY_ID = new HashMap<>();
    public static Integer UNDEFINED_AUTHOR_SEQUENCE = 0;
    public static Integer UNDEFINED_YEAR_SEQUENCE = 0;
    public static String MONGO_URI = "mongodb://user:user@ds119736.mlab.com:19736/si1718-avp-dissertations";
    public static Set<String> STOPWORDS_ES = new HashSet<>();
    public static Set<String> STOPWORDS_EN = new HashSet<>();


    public static void execute() throws IOException {
        loadStopWords();

        MongoClient mongoClient = Utils.connectToMongo(MONGO_URI);
        MongoCollection<org.bson.Document> dissertationsCol = mongoClient
                .getDatabase("si1718-avp-dissertations")
                .getCollection("dissertations");

        List<String> researchersUrls = getResearchersLinks(BASE_URL + "/sisius");

        //List<String> researchersUrls = new ArrayList<>(); researchersUrls.add("/sisius/sis_showpub.php?idpers=10604");

        for(String researcherUrl: researchersUrls){
            // System.out.println(researcherUrl);

            Document doc = Jsoup.parse(new URL(BASE_URL + researcherUrl), 10000);

            String uriId = researcherUrl.split("=")[1]; //get URI id

            // get name of this tutor (researcher)
            String tutor = findByRegex(doc.getElementsByTag("h3"), "^Ficha personal - .*$");
            if(tutor != null)
                tutor = StringUtils.stripAccents(tutor.split("Ficha personal - ")[1]);
            else
                tutor = uriId; // if cannot find name, puts the URI id

            //fixes the document
            Document fixedDoc = fixDoument(doc);
            //System.out.println(fixedDoc);

            // gets the ORCID of the researcher
            String tutorId = findByRegex(
                    fixedDoc
                            .getElementsByTag("p")
                            .first()
                            .getElementsByTag("a"),
                    "^([\\w0-9]{4}-){3}[\\w0-9]{4}$");

            if(tutorId == null)
                tutorId = uriId; // if it hasn't orcid, uses instead the uri id

            // System.out.println(tutorId);

            ID_MAP.put(StringUtils.stripAccents(tutor), tutorId); // keeps the id of this tutor

            // gets the slice which contains the dissertations
            int index = fixedDoc.getAllElements().indexOf(fixedDoc.getAllElements().stream().filter(e -> e.text().equals("Tesis dirigidas y co-dirigidas:")).findFirst().orElse(null));
            int indexNextH3 =
                    fixedDoc.getAllElements().stream().filter(e -> e.tagName().equals("h3")).
                            map(e -> fixedDoc.getAllElements().indexOf(e)).
                            filter(e -> e > index).findFirst().orElse(fixedDoc.getAllElements().size());

            if(index != -1){ // if it found dissertations
                // get the html elements in the slice
                List<Element> dissertationElements = fixedDoc.getAllElements().subList(index, indexNextH3);

                // get the list of authors
                List<String> authors = dissertationElements.stream()
                        .filter(e -> e.tagName().equals("u"))
                        .map(e -> e.text())
                        .collect(Collectors.toList());

                // get the list of dissertations, identified with <p> tag (added while the fixing process)
                List<String> info = dissertationElements.stream()
                        .filter(e -> e.tagName().equals("p"))
                        .map(e -> e.text())
                        .collect(Collectors.toList());

                // generates the list of dissertations
                for(int i = 0; i<authors.size(); i++){
                    String authorStr = "undefined";
                    String[] authorSplit = authors.get(i).split(", ");
                    // if split is not even, it means that the author is undefined
                    if(authorSplit.length %2 == 0){
                        authorStr = StringUtils.stripAccents(authorSplit[1].replace(":","")+ " "+authorSplit[0]);
                    }

                    // let's get the title and year. The year is always in the last position of the string,
                    // just next to the last doc
                    int lastIndex = info.get(i).lastIndexOf(".");

                    String title;
                    Integer year = null;

                    // if there's not any doc, we assume that there's no year
                    if(lastIndex < 0){
                        title = info.get(i);
                    } else {
                        // the year is supposed to be next to the last index
                        String yearStr = info.get(i).substring(lastIndex+2, info.get(i).length());
                        // if the year is numeric, we set the year and the title
                        if(StringUtils.isNumeric(yearStr)){
                            year = new Integer(yearStr);
                            // in this case, the title is between the beginning of the string and the last doc
                            title = info.get(i).substring(0, lastIndex);
                        } else {
                            // otherwise, there's no year, so we take the whole string as the title
                            title = info.get(i);
                        }
                    }

                    // let's generate the dissertation id
                    String idDissertation;
                    String[] idParts = new String[2];
                    // id author is undefined, the author part of the id will be undefined + a sequence number
                    if(authorStr.equals("undefined"))
                        idParts[0] = "undefined"+ ++UNDEFINED_AUTHOR_SEQUENCE;
                    else
                        idParts[0] = authorStr;

                    // if year is null, the year part of the id will be a sequence number
                    if(year == null)
                        idParts[1] = (++UNDEFINED_YEAR_SEQUENCE).toString();
                    else
                        idParts[1] = year.toString();

                    // generates the idDissertation given the author part and the year part
                    idDissertation = generateId(idParts[0]+" "+idParts[1]);

                    // now, let's deal with repeated ids, that can happen either because
                    // this tutor participated in a dissertation that was previously added, or
                    // because the author wrote two dissertations exactly the same year
                    boolean idExists = true;
                    int auxiliarSeq = 0;
                    while(idExists){
                        idExists = DISSERTATIONS_BY_ID.containsKey(idDissertation);
                        if(idExists){ // the ID can already exist for two reasons...
                            Dissertation aux = DISSERTATIONS_BY_ID.get(idDissertation);
                            // because this tutor participated in this dissertation
                            if(aux.getTitle().equals(title)){
                                // some tutors have the same dissertation twice
                                if(!aux.getTutors().contains(tutorId))
                                    aux.addTutor(new Tutor(
                                            "https://si1718-dfr-researchers.herokuapp.com/api/v1/researchers/"+tutorId,
                                            tutor,
                                            "https://si1718-dfr-researchers.herokuapp.com/#!/researchers/"+tutorId+"/view"
                                    ));
                                idExists = false;
                            } else  // or because this author wrote a dissertation exactly the same year
                                idDissertation = generateId(idParts[0]+(++auxiliarSeq)+ " "+idParts[1]);
                        } else { // idExists is set to false. Let's proceed to create the new dissertation

                            boolean authorExists = false;
                            // has this author been processed previously?
                            // if so, the authorStr is the ORCID/URI id of the author
                            // otherwise, authorStr is the name of the author
                            if(ID_MAP.containsKey(StringUtils.stripAccents(authorStr))){
                                authorExists = true;
                                authorStr = ID_MAP.get(StringUtils.strip(authorStr));
                            }

                            List<Tutor> lTutors = new ArrayList<>();
                            lTutors.add(new Tutor(
                                    "https://si1718-dfr-researchers.herokuapp.com/api/v1/researchers/"+tutorId,
                                    tutor,
                                    "https://si1718-dfr-researchers.herokuapp.com/#!/researchers/"+tutorId+"/view"
                            ));

                            Dissertation d = new Dissertation(
                                    lTutors,
                                    authorStr,
                                    title,
                                    year,
                                    idDissertation,
                                    "https://si1718-avp-dissertations.herokuapp.com/#!/dissertations/"+idDissertation+"/view",
                                    generateKeywords(title)
                            );

                            // if the author does not exist, add the dissertation to the
                            // list of dissertations with no author ids
                            if(!authorExists)
                                DISSERTATIONS_WITHOUT_AUTHOR_ID.add(d);

                            // let's keep this dissertation
                            // ONLY if this dissertation is not in the DB
                            BasicDBObject mongoQuery= new BasicDBObject();
                            mongoQuery.put("title", title);

                            long nDiss = dissertationsCol.count(mongoQuery);
                            System.out.println(nDiss);
                            System.out.println(title);
                            if(nDiss == 0)
                                DISSERTATIONS_BY_ID.put(idDissertation, d);
                        }
                    }
                }
            }
        }

        // we've finished the scraping. Now let's check the dissertations whose authors are not
        // represented by their ids
        for(Dissertation aux:DISSERTATIONS_WITHOUT_AUTHOR_ID)
            if(ID_MAP.containsKey(StringUtils.stripAccents(aux.getAuthor())))
                aux.setAuthor(ID_MAP.get( StringUtils.stripAccents(aux.getAuthor())));

        // now let's transform the Java objects into bson documents
        ObjectMapper mapperObj = new ObjectMapper();
        List<org.bson.Document> listDocsMongo = DISSERTATIONS_BY_ID.values().stream()
                .map(x -> {
                    // get Employee object as a json string
                    String jsonStr = null;
                    try {
                        jsonStr = mapperObj.writeValueAsString(x);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return org.bson.Document.parse(jsonStr);
                }).collect(Collectors.toList());

        mongoClient.close();
        System.out.println("Total documents: "+listDocsMongo.size());
        insertDocumentsDB(listDocsMongo, 50);
    }

    private static void insertDocumentsDB(List<org.bson.Document> lDoc, int batchNumber){
        MongoClient client = Utils.connectToMongo(MONGO_URI);
        MongoCollection<org.bson.Document> collection = client.getDatabase("si1718-avp-dissertations").getCollection("newDissertations");
        collection.deleteMany(new BasicDBObject());

        for(int i=0; i<lDoc.size(); i+=batchNumber){
            int j = i + batchNumber;
            if(j>=lDoc.size())
                j = lDoc.size();
            collection.insertMany(lDoc.subList(i, j));
        }

        client.close();
    }

    private static String findByRegex(Elements elements, String regex){
        return elements.stream()
                .map(x -> x.text())
                .filter(x -> x.matches(regex))
                .findFirst().orElse(null);
    }

    private static String generateId(String str){
        Slugify slg = new Slugify();
        return slg.slugify(str);
    }

    private static Document fixDoument(Document doc){
        String str = String.join("\n", Arrays.asList(
                doc.getElementsByAttributeValue("style", "margin-left:1cm")
                        .first()
                        .html()
                        .replace("<v9>", "").replace("</v9>", "")//.replace("&lt;", "").replace("&gt;", "")
                        .split("\n"))
                .stream()
                .map(x -> {
                    if(x.matches("^ *<br>.{3,}$"))
                        return "<p>"+x.replace("<br>", "")+"</p>";
                    else
                        return x;
                })
                .collect(Collectors.toList()));


        return Jsoup.parse(str);
    }

    private static List<String> getResearchersLinks(String url) throws IOException {
        List<String> researchersUrls = new ArrayList<>();

        Document researchers = Jsoup
                .connect(url)
                .data("text2search", "%%%")
                .data("en", "1")
                .data("inside", "1")
                .maxBodySize(10 * 1024 * 1024)
                .post();

        Elements elements = researchers.select("td.data a");
        int i = 0;

        for(Iterator<Element> iterator = elements.iterator(); iterator.hasNext(); ) {
            Element researcher = iterator.next();
            if(i % 2 != 1) {
                String link = researcher.attr("href");
                if(link.contains("sis_showpub.php")) {
                    researchersUrls.add(link);
                }
            }
            i++;
        }
        return researchersUrls;
    }

    private static void loadStopWords(){
        Set<String> stopwordsEs = new HashSet<>();
        Set<String> stopwordsEn = new HashSet<>();

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("stopwords-es.txt"));
            String line;
            while ((line = br.readLine()) != null) {
                stopwordsEs.add(StringUtils.stripAccents(line));
            }

            br = new BufferedReader(new FileReader("stopwords-en.txt"));
            while ((line = br.readLine()) != null) {
                stopwordsEn.add(StringUtils.stripAccents(line));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        STOPWORDS_ES = stopwordsEs;
        STOPWORDS_EN = stopwordsEn;
    }

    private static List<String> generateKeywords(String title) {
        String aux = title.replaceAll("Tesis Doctoral", "");
        List<String> result = new ArrayList<>();
        aux = StringUtils.stripAccents(aux).toLowerCase().replaceAll("[^a-zA-Z\\s,;:.]", "");

        String[] strArr = aux.split("[\\s,;:.]");
        int k = 0;
        boolean incrementK = false;
        for(int i = 0;i< strArr.length;i++){
            if(STOPWORDS_EN.contains(strArr[i]) || STOPWORDS_ES.contains(strArr[i]) || strArr[i].equals("")){
                if(incrementK) {
                    k+=1;
                    incrementK = false;
                }
            } else {
                incrementK = true;
                if(result.size() <= k)
                    result.add(strArr[i]);
                else
                    result.set(k, result.get(k)+" "+strArr[i]);
            }
        }

        return result.stream().filter(x -> x.split(" ").length >1 && x.split(" ").length <4).collect(Collectors.toList());
    }

}
