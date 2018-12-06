package org.infai.senergy.benchmark.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;

public class Importer {

    public static Dataset<Row> importSingleFile(String file, SparkSession spark, boolean inferSchema) {
        return spark.read().option("header", true).option("delimiter", ";").option("inferSchema", inferSchema).csv(file);
    }

    public static Dataset<Row> importAllFiles(String path, SparkSession spark){
        Dataset<Row>[] datasets = new Dataset[10];
        datasets[0] = importSingleFile(path + "30-03-2014_01-05-2014.csv", spark, true);
        datasets[1] = importSingleFile(path + "30-04-2014_01-06-2014.csv", spark, true);
        datasets[2] = importSingleFile(path + "30-06-2014_01-07-2014.csv", spark, true);
        datasets[3] = importSingleFile(path + "30-06-2014_01-08-2014.csv", spark, true);
        datasets[4] = importSingleFile(path + "30-09-2014_01-11-2014.csv", spark, true);
        datasets[5] = importSingleFile(path + "31-05-2014_01-07-2014.csv", spark, true);
        datasets[6] = importSingleFile(path + "31-07-2014_01-09-2014.csv", spark, true);
        datasets[7] = importSingleFile(path + "31-08-2013_31-10-2013.csv", spark, true);
        datasets[8] = importSingleFile(path + "31-08-2014_01-10-2014.csv", spark, true);
        datasets[9] = importSingleFile(path + "31-10-2013_30-03-2014.csv", spark, true);

        Dataset<Row> allFiles = datasets[0].union(datasets[1]).union(datasets[2]).union(datasets[3])
                .union(datasets[4]).union(datasets[5]).union(datasets[6]).union(datasets[7])
                .union(datasets[8]).union(datasets[9]);

        ArrayList<String> joinEx = new ArrayList<>();
        joinEx.add("METER_ID");
        allFiles = allFiles.join(importSingleFile(path + "type.csv", spark, true), convertListToSeq(joinEx), "left");

        allFiles = allFiles.withColumn("CONSUMPTION", allFiles.col("CONSUMPTION").multiply(allFiles.col("FACTOR")));
        return allFiles.select("SEGMENT", "METER_ID", "CONSUMPTION", "TIMESTAMP_UTC").where("OBIS_CODE='1-1:1.8.0*255'");
    }


    private static Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }
}
