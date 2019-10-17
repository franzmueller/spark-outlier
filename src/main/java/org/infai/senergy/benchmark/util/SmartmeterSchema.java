package org.infai.senergy.benchmark.util;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SmartmeterSchema {
    public static StructType getSchema() {
        return new StructType()
                .add("SEGMENT", DataTypes.StringType)
                .add("METER_ID", DataTypes.StringType)
                .add("CONSUMPTION", DataTypes.DoubleType)
                .add("TIMESTAMP_UTC", DataTypes.TimestampType)
                .add("CONSUMPTION_EOY", DataTypes.DoubleType)
                .add("device_id", DataTypes.StringType);
    }

    public static StructType getSchemaString() {
        return new StructType()
                .add("SEGMENT", DataTypes.StringType)
                .add("METER_ID", DataTypes.StringType)
                .add("CONSUMPTION", DataTypes.StringType)
                .add("TIMESTAMP_UTC", DataTypes.StringType)
                .add("CONSUMPTION_EOY", DataTypes.StringType)
                .add("device_id", DataTypes.StringType);
    }
}
