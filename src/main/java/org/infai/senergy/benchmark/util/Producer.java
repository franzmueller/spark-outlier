package org.infai.senergy.benchmark.util;

import org.apache.log4j.LogManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Producer {
    public static void main(String args[]) {
        //Usage check
        String errorMessage = "Usage: org.infai.senergy.benchmark.util.Producer <logging> <path> <hostlist> <topic>\n" +
                "logging = boolean\n" +
                "path = path to Taxi data base folder\n" +
                "hostlist = comma-separated list of kafka host:port\n" +
                "topic = topic to write to";
        if (args.length != 4) {
            System.out.println(errorMessage);
            return;
        }
        //Parameter configuration
        boolean loggingEnabled;
        String path;
        String hostlist;
        String topic;
        try {
            loggingEnabled = Boolean.parseBoolean(args[0]);
            path = args[1];
            hostlist = args[2];
            topic = args[3];
        } catch (Exception e) {
            System.out.println(errorMessage);
            return;
        }

        //Create Session
        SparkSession spark = SparkSession
                .builder()
                .appName("RIoTBench Producer")
                .config("spark.eventLog.enabled", loggingEnabled)
                .getOrCreate();

        for (int year = 2012; year <= 2013; year++) {
            for (int month = 1; month <= 12; month++) {
                try {
                    Dataset<Row> ds = spark.read().json(path + "/json/" + year + "/" + month);
                    ds.write()
                            .format("kafka")
                            .option("kafka.bootstrap.servers", hostlist)
                            .option("topic", topic)
                            .save();

                } catch (Exception e) {
                    LogManager.getLogger("Producer").info("No files found for " + year + "-" + month);
                }
            }
        }
    }
}
