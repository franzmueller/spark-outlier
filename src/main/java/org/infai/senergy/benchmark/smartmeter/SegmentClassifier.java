package org.infai.senergy.benchmark.smartmeter;

import org.apache.spark.ml.*;
import org.apache.spark.ml.classification.Classifier;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.SQLTransformer;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.infai.senergy.benchmark.util.SmartmeterSchema;


public class SegmentClassifier {
    public static void main(String[] args) {
        //Usage check
        String errorMessage = "Usage: org.infai.senergy.benchmark.smartmeter.SegmentClassifier <logging> <hostlist> <inputTopic> <outputTopic> <sigma> <startingOffsets> <maxOffsetsPerTrigger> <shufflePartitions>\n" +
                "logging = boolean\n" +
                "hostlist = comma-separated list of kafka host:port\n" +
                "inputTopic = Topic will be subscribed to.\n" +
                "outputTopic = Output topic, where values will be written to\n" +
                "startingOffsets = Which Kafka Offset to use. Use earliest or latest\n" +
                "maxOffsetsPerTrigger = How many messages should be consumed at once (max)\n" +
                "shufflePartitions = How many shuffle partitions Spark should use (default is 200)";

        if (args.length != 7) {
            System.out.println(errorMessage);
            return;
        }
        //Parameter configuration
        boolean loggingEnabled;
        String hostlist, inputTopic, outputTopic, startingOffsets;
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
        } catch (Exception e) {
            System.out.println(errorMessage);
            return;
        }

        //Create Session
        SparkSession spark = SparkSession
                .builder()
                .appName("SmartMeter SegmentClassifier")
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
        StructType schema = SmartmeterSchema.getSchema();

        //Parse Kafka value to Dataframe (via json)
        Dataset<Row> df = ds.select(functions.from_json(ds.col("value").cast(DataTypes.StringType), schema)
                .as("data"))
                .select("data.*");


        //Create indexers
        StringIndexer segmentIndexer = new StringIndexer()
                .setInputCol("SEGMENT")
                .setOutputCol("indexedSEGMENT");
        StringIndexer meterIndexer = new StringIndexer()
                .setInputCol("METER_ID")
                .setOutputCol("indexedMETER_ID");

        Transformer sqlTransformer = new SQLTransformer().setStatement("SELECT CONSUMPTION, SEGMENT, indexedMETER, unix_timestamp(TIMESTAMP_UTC) AS unixTIMESTAMP_UTC FROM __THIS__");

        //Create assembler
        String[] featuresCols = {"indexedMETER_ID", "CONSUMPTION", "unixTIMESTAMP_UTC"};
        VectorAssembler assembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("FEATURES");


        Pipeline prep = new Pipeline().setStages(new PipelineStage[]{meterIndexer, sqlTransformer, assembler});
        Dataset<Row> prepared = prep.fit(df).transform(df);

        /*
        //Index data
        df = meterIndexer.fit(df).transform(df);
        df = timestampIndexer.fit(df).transform(df);
        df = assembler.transform(df);

         */

        //Create training data and index segments
        Dataset<Row> trainingData = prepared.where(df.col("SEGMENT").isNotNull());
        trainingData = segmentIndexer.fit(trainingData).transform(trainingData);

        //Create test data
        Dataset<Row> testData = df.where(df.col("SEGMENT").isNull());

        //Create classifier
        Classifier classifier = new RandomForestClassifier();
        Predictor predictor = classifier.setLabelCol("indexedSEGMENT");
        predictor = predictor.setFeaturesCol("FEATURES");
        predictor = predictor.setPredictionCol("PREDICTION");

        PredictionModel model = predictor.fit(trainingData);

        Dataset segmented = model.transform(testData);


        //Write outliers to kafka
        segmented.toJSON()
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
