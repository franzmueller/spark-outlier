package org.infai.senergy.benchmark.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Converter {
    public static void main(String[] args) {
        //Usage check
        String errorMessage = "Usage: org.infai.senergy.benchmark.util.Converter <logging> <path>\n" +
                "logging = boolean\n" +
                "path = path to smartmeter data base folder\n";

        if (args.length != 2) {
            System.out.println(errorMessage);
            return;
        }
        //Parameter configuration
        boolean loggingEnabled;
        String path;
        try {
            loggingEnabled = Boolean.parseBoolean(args[0]);
            path = args[1];
        } catch (Exception e) {
            System.out.println(errorMessage);
            return;
        }

        //Create Session
        SparkSession spark = SparkSession
                .builder()
                .appName("SmartMeter Converter")
                .config("spark.eventLog.enabled", loggingEnabled)
                .getOrCreate();


        Dataset<Row> ds = Importer.importAllFiles(path, spark);
        ds = ds.repartition(1);

        ds.write()
                .option("compression", "gzip")
                .json(path + "json");
        ds.write()
                .option("header", "true").option("compression","gzip")
                .csv(path + "csv");

    }

}
