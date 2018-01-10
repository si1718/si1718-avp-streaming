package data.streaming.threads.jobs;

import data.streaming.threads.FlinkKafkaProducer;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class FlinkKafkaProducerJob implements org.quartz.Job {

    public FlinkKafkaProducerJob() {
    }

    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            FlinkKafkaProducer.main();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Execution completed. Next will take part on " + context.getTrigger().getNextFireTime());
    }
}