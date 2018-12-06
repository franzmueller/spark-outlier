package org.infai.senergy.benchmark.taxi.tasks.predictive;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LinRegTrainTaxi extends AbstractModelTrainer implements Runnable {


    public LinRegTrainTaxi(SparkSession spark, String hostlist, String topics, long updateInterval) {
        super(spark, hostlist, topics, updateInterval);
    }

    protected LinearRegressionModel fit(Dataset<Row> dataset) {
        LinearRegression linReg = new LinearRegression()
                .setLabelCol(" fare_amount")
                .setFeaturesCol("features");
        dataset = prepareFeatures(dataset);
        return linReg.fit(dataset);
    }

    public Dataset<Row> prepareFeatures(Dataset<Row> dataset) {
        VectorAssembler va = new VectorAssembler()
                .setInputCols(new String[]{" trip_time_in_secs", " trip_distance"})
                .setOutputCol("features");
        return va.transform(dataset);
    }
}

