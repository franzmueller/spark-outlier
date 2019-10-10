package org.infai.senergy.benchmark.smartmeter.util;

import org.apache.spark.ml.*;
import org.apache.spark.ml.classification.Classifier;
import org.apache.spark.ml.feature.SQLTransformer;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.sources.StreamSinkProvider;
import org.apache.spark.sql.streaming.OutputMode;
import scala.collection.Seq;
import scala.collection.immutable.Map;

public class SegmentClassifierSinkProvider implements StreamSinkProvider {
    String hostlist, outputTopic;
    Classifier classifier;

    public SegmentClassifierSinkProvider(String hostlist, String outputTopic, Classifier classifier) {
        super();
        this.hostlist = hostlist;
        this.outputTopic = outputTopic;
        this.classifier = classifier;
    }

    @Override
    public Sink createSink(SQLContext sqlContext, Map<String, String> map, Seq<String> seq, OutputMode outputMode) {
        return new SegmentClassifierSink(sqlContext, hostlist, outputTopic, classifier);
    }
}

class SegmentClassifierSink implements Sink {

    Dataset<Row> training;
    SQLContext sqlContext;
    String hostlist, outputTopic;
    Classifier classifier;

    public SegmentClassifierSink(SQLContext sqlContext, String hostlist, String outputTopic, Classifier classifier) {
        super();
        this.hostlist = hostlist;
        this.outputTopic = outputTopic;
        this.classifier = classifier;
        this.sqlContext = sqlContext;
        training = sqlContext.emptyDataFrame();
    }

    @Override
    public void addBatch(long l, Dataset<Row> dataset) {
        training = training.join(dataset.where(dataset.col("SEGMENT").isNotNull()));
        //Create indexers
        StringIndexer segmentIndexer = new StringIndexer()
                .setInputCol("SEGMENT")
                .setOutputCol("indexedSEGMENT");
        StringIndexer meterIndexer = new StringIndexer()
                .setInputCol("METER_ID")
                .setOutputCol("indexedMETER_ID");

        Transformer sqlTransformer = new SQLTransformer().setStatement("SELECT CONSUMPTION, indexedSEGMENT, indexedMETER_ID, unix_timestamp(TIMESTAMP_UTC) AS unixTIMESTAMP_UTC FROM __THIS__");

        //Create assembler
        String[] featuresCols = {"indexedMETER_ID", "CONSUMPTION", "unixTIMESTAMP_UTC"};
        VectorAssembler assembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("FEATURES");

        Predictor predictor = classifier.setLabelCol("indexedSEGMENT");
        predictor = predictor.setFeaturesCol("FEATURES");
        predictor = predictor.setPredictionCol("PREDICTION");

        Pipeline trainingPipeline = new Pipeline().setStages(new PipelineStage[]{meterIndexer, segmentIndexer, sqlTransformer, assembler, predictor});
        PipelineModel trainingPipelineModel = trainingPipeline.fit(training);


        trainingPipelineModel.transform(dataset)
                .toJSON()
                .writeStream()
                .format("kafka")
                .option("checkpointLocation", "checkpoints/smartmeter/outlierdetecion")
                .option("kafka.bootstrap.servers", hostlist)
                .option("topic", outputTopic)
                .start();
    }
}
