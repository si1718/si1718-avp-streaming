
package data.streaming.threads;

import java.util.Properties;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import data.streaming.aux.ValidTagsTweetEndpoIntinitializer;
import data.streaming.utils.LoggingFactory;
import data.streaming.utils.Utils;

public class FlinkKafkaProducer {

    private static final Integer PARALLELISM = 2;
    private static JobExecutionResult jobExecutionResult;

    public static void main(String... args) throws Exception {
        String[] tags = Utils.getKeywords();

        TwitterSource twitterSource = new TwitterSource(LoggingFactory.getTwitterCredentias());

        // Establecemos el filtro
        twitterSource.setCustomEndpointInitializer(new ValidTagsTweetEndpoIntinitializer(tags));

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(PARALLELISM);

        // Añadimos la fuente y generamos el stream como la salida de las llamadas
        // asíncronas para salvar los datos en MongoDB
        DataStream<String> stream = env.addSource(twitterSource);

        Properties props = LoggingFactory.getCloudKarafkaCredentials();

        FlinkKafkaProducer010.FlinkKafkaProducer010Configuration<String> config = FlinkKafkaProducer010
                .writeToKafkaWithTimestamps(stream, props.getProperty("CLOUDKARAFKA_TOPIC").trim(), new SimpleStringSchema(),
                        props);
        config.setWriteTimestampToKafka(false);
        config.setLogFailuresOnly(false);
        config.setFlushOnCheckpoint(true);

        stream.print();

        env.execute("Twitter Streaming Producer");
    }

}
