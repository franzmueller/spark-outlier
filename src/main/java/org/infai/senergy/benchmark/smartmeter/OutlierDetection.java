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
import org.infai.senergy.benchmark.smartmeter.util.RowWithDiff;
import org.infai.senergy.benchmark.smartmeter.util.TimestampDoublePair;
import org.infai.senergy.benchmark.util.SmartmeterSchema;


public class OutlierDetection {
    public static void main(String[] args) throws Exception {
        //Usage check
        String errorMessage = "Usage: org.infai.senergy.benchmark.smartmeter.StreamingBenchmark <logging> <hostlist> <topics> <updateInterval>\n" +
                "logging = boolean\n" +
                "hostlist = comma-separated list of kafka host:port\n" +
                "topic = Topic to use. Topic will be subscribed to and will use <topic>-<task> as publish topic.\n" +
                "updateInterval = Integer value in millis after which models should be updated.";

        if (args.length != 4) {
            System.out.println(errorMessage);
            return;
        }
        //Parameter configuration
        boolean loggingEnabled;
        String hostlist;
        String topics;
        int updateInterval;
        try {
            loggingEnabled = Boolean.parseBoolean(args[0]);
            hostlist = args[1];
            topics = args[2];
            updateInterval = Integer.parseInt(args[3]);
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
                .option("subscribe", topics)
                .load();

        //Prepare the schema
        StructType schema = SmartmeterSchema.getSchema();

        //Parse Kafka value to Dataframe (via json)
        Dataset<Row> df = ds.select(functions.from_json(ds.col("value").cast(DataTypes.StringType), schema)
                .as("data"))
                .select("data.*");

        //Add CONSUMPTION_DIFF column
        Dataset<Row> diffed = df.groupByKey((MapFunction) new ConsumptionMapper(), Encoders.STRING())
                .flatMapGroupsWithState(new FlatDiff(), OutputMode.Append(), Encoders.bean(TimestampDoublePair.class), Encoders.bean(RowWithDiff.class), GroupStateTimeout.NoTimeout());

        //df.writeStream().format("console").start(); //TODO Just to see which data arrived
        //diffed.writeStream().format("console").start(); //TODO just to see diffs

        diffed.groupBy("METER_ID").agg(functions.avg("DIFF").as("AVG_DIFF"), functions.stddev("DIFF").as("STDDEV_DIFF")).writeStream().format("memory")
                .queryName("stats").outputMode(OutputMode.Complete()).start();
        diffed.join(spark.sql("SELECT * FROM stats"), "METER_ID").writeStream().format("console").start();

        //diffed.groupBy("METER_ID").avg("DIFF").writeStream().format("console").outputMode(OutputMode.Complete()).start();
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
