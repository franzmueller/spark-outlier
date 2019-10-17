package org.infai.senergy.benchmark.util;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SmartmeterSchema {
    public static StructType getSchema() {
        return new StructType()
                .add("SEGMENT", DataTypes.StringType.asNullable())
                .add("METER_ID", DataTypes.StringType)
                .add("CONSUMPTION", DataTypes.DoubleType)
                .add("TIMESTAMP_UTC", DataTypes.TimestampType)
                .add("CONSUMPTION_EOY", DataTypes.DoubleType)
                .add("device_id", DataTypes.StringType.asNullable());
    }
}
