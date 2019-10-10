package org.infai.senergy.benchmark.smartmeter;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
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
        String errorMessage = "Usage: org.infai.senergy.benchmark.smartmeter.SegmentClassifier <logging> <hostlist> <inputTopicTraining> <inputTopicTest> <outputTopic> <sigma> <startingOffsets> <maxOffsetsPerTrigger> <shufflePartitions>\n" +
                "logging = boolean\n" +
                "hostlist = comma-separated list of kafka host:port\n" +
                "inputTopicTraining = Topic of training data.\n" +
                "inputTopicTest = Topic of test data.\n" +
                "outputTopic = Output topic, where values will be written to\n" +
                "startingOffsets = Which Kafka Offset to use. Use earliest or latest\n" +
                "maxOffsetsPerTrigger = How many messages should be consumed at once (max)\n" +
                "shufflePartitions = How many shuffle partitions Spark should use (default is 200)" +
                "maxBins = To be used for RandomForest config" +
                "maxMemory = Memory to be used for classification";

        if (args.length != 10) {
            System.out.println(errorMessage);
            return;
        }
        //Parameter configuration
        boolean loggingEnabled;
        String hostlist, inputTopicTraining, inputTopicTest, outputTopic, startingOffsets;
        int shufflePartitions, maxBins, maxMemory;
        long maxOffsetsPerTrigger;
        try {
            loggingEnabled = Boolean.parseBoolean(args[0]);
            hostlist = args[1];
            inputTopicTraining = args[2];
            inputTopicTest = args[3];
            outputTopic = args[4];
            startingOffsets = args[5];
            maxOffsetsPerTrigger = Long.parseLong(args[6]);
            shufflePartitions = Integer.parseInt(args[7]);
            maxBins = Integer.parseInt(args[8]);
            maxMemory = Integer.parseInt(args[9]);
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
                .config("spark.sql.streaming.checkpointLocation", "checkpoints")
                .getOrCreate();

        //TRAINING PHASE

        //Create DataSet representing the stream of input lines from kafka
        Dataset<Row> trainingData = spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", hostlist)
                .option("subscribe", inputTopicTraining)
                .option("startingOffsets", startingOffsets)
                .load();

        //Prepare the schema
        StructType schema = SmartmeterSchema.getSchema();

        //Parse Kafka value to Dataframe (via json)
        trainingData = trainingData.select(functions.from_json(trainingData.col("value").cast(DataTypes.StringType), schema)
                .as("data"))
                .select("data.*");

        StringIndexer segmentIndexer = new StringIndexer()
                .setInputCol("SEGMENT")
                .setOutputCol("indexedSEGMENT")
                .setHandleInvalid("keep");
        StringIndexer meterIndexer = new StringIndexer()
                .setInputCol("METER_ID")
                .setOutputCol("indexedMETER_ID")
                .setHandleInvalid("keep");

        Transformer sqlTransformer = new SQLTransformer().setStatement("SELECT CONSUMPTION, indexedSEGMENT, indexedMETER_ID, unix_timestamp(TIMESTAMP_UTC) AS unixTIMESTAMP_UTC FROM __THIS__");

        //Create assembler
        String[] featuresCols = {"indexedMETER_ID", "CONSUMPTION", "unixTIMESTAMP_UTC"};
        VectorAssembler assembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("FEATURES");

        RandomForestClassifier classifier = new RandomForestClassifier();
        classifier = classifier.setProbabilityCol("PREDICTION_PROB");
        classifier = classifier.setLabelCol("indexedSEGMENT");
        classifier = classifier.setFeaturesCol("FEATURES");
        classifier = classifier.setPredictionCol("PREDICTION");
        classifier = classifier.setMaxBins(maxBins);
        classifier = classifier.setMaxMemoryInMB(maxMemory);

        Pipeline trainingPipeline = new Pipeline().setStages(new PipelineStage[]{meterIndexer, segmentIndexer, sqlTransformer, assembler, classifier});
        PipelineModel model = trainingPipeline.fit(trainingData);


        //TEST PHASE

        //Create DataSet representing the stream of input lines from kafka
        Dataset<Row> testData = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", hostlist)
                .option("subscribe", inputTopicTest)
                .option("startingOffsets", startingOffsets)
                .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
                .load();

        //Parse Kafka value to Dataframe (via json)
        testData = testData.select(functions.from_json(testData.col("value").cast(DataTypes.StringType), schema)
                .as("data"))
                .select("data.*");

        model.transform(testData)
                .toJSON()
                .writeStream()
                .format("kafka")
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
