package org.infai.senergy.benchmark.smartmeter;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.infai.senergy.benchmark.smartmeter.util.ConsumptionMapper;
import org.infai.senergy.benchmark.smartmeter.util.FlatDiff;
import org.infai.senergy.benchmark.smartmeter.util.GroupStateContainer;
import org.infai.senergy.benchmark.smartmeter.util.RowWithDiff;
import org.infai.senergy.benchmark.util.SmartmeterSchema;


public class OutlierDetection {
    public static void main(String[] args) {
        //Usage check
        String errorMessage = "Usage: org.infai.senergy.benchmark.smartmeter.StreamingBenchmark <logging> <hostlist> <inputTopic> <outputTopic> <sigma>\n" +
                "logging = boolean\n" +
                "hostlist = comma-separated list of kafka host:port\n" +
                "inputTopic = Topic will be subscribed to.\n" +
                "outputTopic = Output topic, where values will be written to\n" +
                "sigma = Integer value. How many standard deviations above or bellow average is considered an outlier?";

        if (args.length != 5) {
            System.out.println(errorMessage);
            return;
        }
        //Parameter configuration
        boolean loggingEnabled;
        String hostlist, inputTopic, outputTopic;
        int sigma;
        try {
            loggingEnabled = Boolean.parseBoolean(args[0]);
            hostlist = args[1];
            inputTopic = args[2];
            outputTopic = args[3];
            sigma = Integer.parseInt(args[4]);
        } catch (Exception e) {
            System.out.println(errorMessage);
            return;
        }

        //Create Session
        SparkSession spark = SparkSession
                .builder()
                .appName("SmartMeter OutlierDetection")
                .config("spark.eventLog.enabled", loggingEnabled)
                .getOrCreate();

        //Create DataSet representing the stream of input lines from kafka
        Dataset<Row> ds = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", hostlist)
                .option("subscribe", inputTopic)
                .option("startingOffsets", "earliest")
                .load();

        //Prepare the schema
        StructType schema = SmartmeterSchema.getSchema();

        //Parse Kafka value to Dataframe (via json)
        Dataset<Row> df = ds.select(functions.from_json(ds.col("value").cast(DataTypes.StringType), schema)
                .as("data"))
                .select("data.*");

        //Add CONSUMPTION_DIFF column
        Dataset<Row> diffed = df.groupByKey((MapFunction) new ConsumptionMapper(), Encoders.STRING())
                .flatMapGroupsWithState(new FlatDiff(), OutputMode.Append(), Encoders.bean(GroupStateContainer.class), Encoders.bean(RowWithDiff.class), GroupStateTimeout.NoTimeout());

        //Detect outliers
        Dataset<Row> outliers = diffed.withColumn("SIGMA", diffed.col("DIFF").minus(diffed.col("AVG_DIFF")).divide(diffed.col("STDDEV_DIFF")))
                .where("SIGMA > " + sigma + " OR SIGMA < " + (sigma * -1));

        //Write outliers to kafka
        outliers.toJSON()
                .writeStream()
                .format("kafka")
                .option("checkpointLocation", "checkpoints/smartmeter/outlierdetecion")
                .option("kafka.bootstrap.servers", hostlist)
                .option("topic", outputTopic)
                .start();

        // Wait for termination
        try {
            spark.streams().awaitAnyTermination();
        } catch (StreamingQueryException sqe) {
            System.out.println("Encountered StreamingQueryException while waiting for Termination. Did you try to exit the program?");
            sqe.printStackTrace();
        } finally {
            spark.close();
        }
    }
}
