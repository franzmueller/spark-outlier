package org.infai.senergy.benchmark.taxi.tasks.predictive;

import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;

public class DecisionTreeTrainTaxi extends AbstractModelTrainer implements Runnable {

    public DecisionTreeTrainTaxi(SparkSession spark, String hostlist, String topics, long updateInterval) {
        super(spark, hostlist, topics, updateInterval);
    }

    @Override
    protected DecisionTreeClassificationModel fit(Dataset<Row> dataset) {
        DecisionTreeClassifier dtc = new DecisionTreeClassifier();

        //Set labels
        dtc.setFeaturesCol("features");
        dtc.setLabelCol("DTTlabel");

        //Prepare features
        dataset = prepareFeatures(dataset);

        //Prepare label
        dataset = dataset.withColumn("DTTlabel", functions.ntile(4).over(Window.orderBy(" total_amount")));

        return dtc.fit(dataset);
    }

    public Dataset<Row> prepareFeatures(Dataset<Row> dataset) {
        VectorAssembler va = new VectorAssembler()
                .setInputCols(new String[]{" trip_time_in_secs", " trip_distance", " fare_amount"})
                .setOutputCol("features");
        return va.transform(dataset);
    }
}

