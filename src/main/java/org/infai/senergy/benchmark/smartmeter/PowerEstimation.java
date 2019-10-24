package org.infai.senergy.benchmark.smartmeter;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.infai.senergy.benchmark.smartmeter.estimation.PowerEstimator;
import org.infai.senergy.benchmark.smartmeter.estimation.PowerStateContainer;
import org.infai.senergy.benchmark.smartmeter.estimation.RowWithEstimation;
import org.infai.senergy.benchmark.smartmeter.util.ConsumptionMapper;
import org.infai.senergy.benchmark.util.SmartmeterSchema;


public class PowerEstimation {
    public static void main(String[] args) {
        //Usage check
        String errorMessage = "Usage: org.infai.senergy.benchmark.smartmeter.PowerEstimation <logging> <hostlist>" +
                " <inputTopic> <outputTopic> <startingOffsets> <maxOffsetsPerTrigger> <shufflePartitions> <debug>" +
                " <checkpointLocation>\n" +

                "logging = boolean\n" +
                "hostlist = comma-separated list of kafka host:port\n" +
                "inputTopic = Topic will be subscribed to.\n" +
                "outputTopic = Output topic, where values will be written to\n" +
                "startingOffsets = Which Kafka Offset to use. Use earliest or latest\n" +
                "maxOffsetsPerTrigger = How many messages should be consumed at once (max)\n" +
                "shufflePartitions = How many shuffle partitions Spark should use (default is 200)\n" +
                "debug = whether or not to print results to console\n" +
                "checkpointLocation = directory on a distributed file system";

        if (args.length != 9) {
            System.out.println(errorMessage);
            return;
        }
        //Parameter configuration
        boolean loggingEnabled, debug;
        String hostlist, inputTopic, outputTopic, startingOffsets, checkpointLocation;
        int shufflePartitions;
        long maxOffsetsPerTrigger;
        try {
            loggingEnabled = Boolean.parseBoolean(args[0]);
            hostlist = args[1];
            inputTopic = args[2];
            outputTopic = args[3];
            startingOffsets = args[4];
            maxOffsetsPerTrigger = Long.parseLong(args[5]);
            shufflePartitions = Integer.parseInt(args[6]);
            debug = Boolean.parseBoolean(args[7]);
            checkpointLocation = args[8];
        } catch (Exception e) {
            System.out.println(errorMessage);
            return;
        }

        //Create Session
        SparkSession spark = SparkSession
                .builder()
                .appName("SmartMeter PowerEstimation")
                .config("spark.eventLog.enabled", loggingEnabled)
                .config("spark.sql.shuffle.partitions", shufflePartitions)
                .getOrCreate();

        //Create DataSet representing the stream of input lines from kafka
        Dataset<Row> ds = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", hostlist)
                .option("subscribe", inputTopic)
                .option("startingOffsets", startingOffsets)
                .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
                .load();

        //Prepare the schema
        StructType schema = SmartmeterSchema.getSchemaString();

        //Parse Kafka value to Dataframe (via json)
        Dataset<Row> df = ds.select(functions.from_json(ds.col("value").cast(DataTypes.StringType), schema)
                .as("data"))
                .select("data.*");

        //apply schema manually
        df = df.selectExpr("SEGMENT", "METER_ID", "cast(CONSUMPTION as double)", "cast(TIMESTAMP_UTC as timestamp)", "cast(CONSUMPTION_EOY as double)");


        //Add PREDICTION & PREDICTION_TIMESTAMP column
        Dataset<Row> predicted = df.groupByKey((MapFunction) new ConsumptionMapper(), Encoders.STRING())
                .flatMapGroupsWithState(new PowerEstimator(), OutputMode.Append(), Encoders.javaSerialization(PowerStateContainer.class), Encoders.bean(RowWithEstimation.class), GroupStateTimeout.NoTimeout());


        //Write outliers to kafka
        predicted.toJSON()
                .writeStream()
                .format("kafka")
                .option("checkpointLocation", checkpointLocation + "/power_estimation")
                .option("kafka.bootstrap.servers", hostlist)
                .option("topic", outputTopic)
                .start();

        if (debug) {
            ds.writeStream().format("console").outputMode("append").start();
            df.printSchema();
            df.writeStream().format("console").outputMode("append").start();
            predicted.writeStream().format("console").outputMode("append").start();
        }

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
