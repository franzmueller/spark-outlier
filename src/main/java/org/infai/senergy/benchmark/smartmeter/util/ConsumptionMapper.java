package org.infai.senergy.benchmark.smartmeter.util;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class ConsumptionMapper implements MapFunction<Row, String> {
    @Override
    public String call(Row row) {
        return row.getString(row.fieldIndex("METER_ID"));
    }
}
