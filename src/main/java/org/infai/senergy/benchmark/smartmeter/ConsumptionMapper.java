package org.infai.senergy.benchmark.smartmeter;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ConsumptionMapper implements MapFunction<Dataset<Row>, Dataset<Row>> {
    @Override
    public Dataset<Row> call(Dataset<Row> value) throws Exception {
        return value.select("METER_ID");
    }
}
