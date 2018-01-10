package data.streaming.threads;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import data.streaming.dto.TweetDTO;
import data.streaming.utils.Utils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import data.streaming.utils.LoggingFactory;
import org.apache.flink.util.Collector;
import org.bson.Document;

public class FlinkKafkaConsumer {


	public static void main(String... args) throws Exception {

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = LoggingFactory.getCloudKarafkaCredentials();


		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(props.getProperty("CLOUDKARAFKA_TOPIC").trim(), new SimpleStringSchema(), props);
		consumer.setStartFromLatest();

		DataStream<String> stream = env.addSource(consumer);

		// creo una ventana temporal. Cada 10 segundos empaqueta la informacion
		//AllWindowFunction<String, String, TimeWindow> function = new AllWindowFunctionImpl
		stream.timeWindowAll(Time.minutes(10))
			.apply(new AllWindowFunction<String, String, TimeWindow>() {
				@Override
				public void apply(TimeWindow timeWindow, Iterable<String> iterable, Collector<String> collector) throws Exception {
					MongoClient client = Utils.connectToMongo("mongodb://user:user@ds119736.mlab.com:19736/si1718-avp-dissertations");
					MongoCollection<Document> collection = client.getDatabase("si1718-avp-dissertations").getCollection("tweetsStats");
					List<TweetDTO> tweets = new LinkedList<>();
					List<Document> tweetStats = new LinkedList<>();
					Calendar today = Calendar.getInstance();


					for(String s: iterable)
						if(Utils.isValid(s))
							tweets.add(Utils.createTweetDTO(s));


					for(String keyword: Utils.getKeywords()){
						int count = (int) tweets.stream()
								.filter(x ->
									Utils.isSameDay(Utils.getDateFromTweet(x), today) &&
											(x.getText().toLowerCase().contains(keyword.toLowerCase()) ||
											x.getText().toLowerCase().contains("#"+keyword.replaceAll("\\s+", ""))) )
								.count();
						Document stats = new Document();

						Document dateDoc = new Document();
						dateDoc.put("year", today.get(Calendar.YEAR));
						dateDoc.put("month", today.get(Calendar.MONTH) + 1);
						dateDoc.put("day", today.get(Calendar.DAY_OF_MONTH));

						stats.put("keyword", keyword);
						stats.put("count", count);
						stats.put("date", dateDoc);

						tweetStats.add(stats);
					}
					System.out.println(tweetStats);
					collection.insertMany(tweetStats);
					client.close();

					for(String s:iterable)
						collector.collect(s);
				}
			});

//		stream.print();

		// execute program
		env.execute("Twitter Streaming Consumer");
	}

}
