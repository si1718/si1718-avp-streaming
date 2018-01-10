package data.streaming.threads.jobs;

import data.streaming.threads.FlinkKafkaConsumer;
import data.streaming.threads.FlinkKafkaProducer;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class FlinkKafkaConsumerJob implements org.quartz.Job {

    public FlinkKafkaConsumerJob() {
    }

    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            //FlinkKafkaConsumer.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Execution completed. Next will take part on " + context.getTrigger().getNextFireTime());
    }
}