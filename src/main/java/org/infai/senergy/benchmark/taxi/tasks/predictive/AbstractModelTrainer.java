package org.infai.senergy.benchmark.taxi.tasks.predictive;

import org.apache.log4j.LogManager;
import org.apache.spark.ml.Model;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.infai.senergy.benchmark.util.TaxiSchema;


public abstract class AbstractModelTrainer implements Runnable {
    private SparkSession spark;
    private String hostlist;
    private String topics;
    private Model model;
    private long updateInterval;
    private int modelVersion;

    protected AbstractModelTrainer(SparkSession spark, String hostlist, String topics, long updateInterval) {
        this.spark = spark;
        this.hostlist = hostlist;
        this.topics = topics;
        this.updateInterval = updateInterval;
        modelVersion = 0;
    }

    public Model getLatestModel() throws Exception {
        if (!isModelReady())
            throw new Exception("Model not yet trained!");
        return model;
    }

    public boolean isModelReady() {
        return model != null;
    }

    protected abstract Model fit(Dataset<Row> dataset);

    private void retrainModel() {
        Dataset<Row> kafkabatch = spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", hostlist)
                .option("subscribe", topics)
                .load();

        //Prepare the schema
        StructType schema = TaxiSchema.getSchemaAllStrings();

        //Parse data from kafka
        kafkabatch = kafkabatch.select(functions.from_json(kafkabatch.col("value").cast(DataTypes.StringType), schema)
                .as("data"))
                .select("data.*");
        kafkabatch = TaxiSchema.applySchema(kafkabatch);

        //Train LinearRegression & Update model
        model = fit(kafkabatch);
        modelVersion++;
    }

    @Override
    public void run() {
        long startTime;
        long sleepTime;
        while (true) {
            startTime = System.currentTimeMillis();
            retrainModel();
            LogManager.getLogger(this.getClass().getSimpleName()).info("Updated Model! Version: " + modelVersion);
            sleepTime = updateInterval - (System.currentTimeMillis() - startTime);
            try {
                Thread.sleep(sleepTime < 0 ? 0 : sleepTime);
            } catch (InterruptedException e) {
                LogManager.getLogger("LinRegTrainTaxi").info("Interrupted while waiting for new training run!");
            }
        }
    }
}



