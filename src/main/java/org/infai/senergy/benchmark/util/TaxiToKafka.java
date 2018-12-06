package org.infai.senergy.benchmark.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class TaxiToKafka {

    public static void main(String args[]) {
        //Usage check
        String errorMessage = "Usage: org.infai.senergy.benchmark.util.TaxiToKafka <logging> <size> <path> <hostlist> <topic>\n" +
                "logging = boolean\n" +
                "size = int from -1 to 2\n" +
                "path = path to Taxi data base folder\n" +
                "hostlist = comma-separated list of kafka host:port\n" +
                "topic = topic to write to";
        if (args.length != 5) {
            System.out.println(errorMessage);
            return;
        }
        //Parameter configuration
        boolean loggingEnabled;
        int size;
        String path;
        String hostlist;
        String topic;
        try {
            loggingEnabled = Boolean.parseBoolean(args[0]);
            size = Integer.parseInt(args[1]);
            path = args[2];
            hostlist = args[3];
            topic = args[4];
        } catch (Exception e) {
            System.out.println(errorMessage);
            return;
        }

        //Create Session
        SparkSession spark = SparkSession
                .builder()
                .appName("RIoTBench TaxiToKafka")
                .config("spark.eventLog.enabled", loggingEnabled)
                .getOrCreate();

        //Import csv
        Dataset<Row> dataCsv=null;
        /* TODO
        switch (size) {
            case -1:
                dataCsv = Importer.importSample(path, spark, false);
                break;
            case 0:
                dataCsv = Importer.importMonth(2013, 1, path, spark, false);
                break;
            case 1:
                dataCsv = Importer.importYear(2012, path, spark, false);
                break;
            case 2:
            default:
                dataCsv = Importer.importAll(path, spark, false);
        }*/

        //Convert csv to json
        Dataset<String> dataJson = dataCsv.toJSON();

        dataJson.write()
                .format("kafka")
                .option("kafka.bootstrap.servers", hostlist)
                .option("topic", topic)
                .save();
    }
}
