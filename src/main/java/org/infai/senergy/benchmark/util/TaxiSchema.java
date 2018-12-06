package org.infai.senergy.benchmark.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class TaxiSchema {
    public static StructType getSchema() {
        return new StructType()
                .add("medallion", DataTypes.IntegerType)
                .add(" hack_license", DataTypes.IntegerType)
                .add(" vendor_id", DataTypes.IntegerType)
                .add(" pickup_datetime", DataTypes.TimestampType)
                .add(" rate_code", DataTypes.IntegerType)
                .add(" store_and_fwd_flag", DataTypes.StringType)
                .add(" dropoff_datetime", DataTypes.TimestampType)
                .add(" passenger_count", DataTypes.IntegerType)
                .add(" trip_time_in_secs", DataTypes.IntegerType)
                .add(" trip_distance", DataTypes.DoubleType)
                .add(" pickup_longitude", DataTypes.DoubleType)
                .add(" pickup_latitude", DataTypes.DoubleType)
                .add(" dropoff_longitude", DataTypes.DoubleType)
                .add(" dropoff_latitude", DataTypes.DoubleType)
                .add(" payment_type", DataTypes.StringType)
                .add(" fare_amount", DataTypes.DoubleType)
                .add(" surcharge", DataTypes.DoubleType)
                .add(" mta_tax", DataTypes.DoubleType)
                .add(" tip_amount", DataTypes.DoubleType)
                .add(" tolls_amount", DataTypes.DoubleType)
                .add(" total_amount", DataTypes.DoubleType);
    }

    public static StructType getSchemaAllStrings() {
        return new StructType()
                .add("medallion", DataTypes.StringType)
                .add(" hack_license", DataTypes.StringType)
                .add(" vendor_id", DataTypes.StringType)
                .add(" pickup_datetime", DataTypes.StringType)
                .add(" rate_code", DataTypes.StringType)
                .add(" store_and_fwd_flag", DataTypes.StringType)
                .add(" dropoff_datetime", DataTypes.StringType)
                .add(" passenger_count", DataTypes.StringType)
                .add(" trip_time_in_secs", DataTypes.StringType)
                .add(" trip_distance", DataTypes.StringType)
                .add(" pickup_longitude", DataTypes.StringType)
                .add(" pickup_latitude", DataTypes.StringType)
                .add(" dropoff_longitude", DataTypes.StringType)
                .add(" dropoff_latitude", DataTypes.StringType)
                .add(" payment_type", DataTypes.StringType)
                .add(" fare_amount", DataTypes.StringType)
                .add(" surcharge", DataTypes.StringType)
                .add(" mta_tax", DataTypes.StringType)
                .add(" tip_amount", DataTypes.StringType)
                .add(" tolls_amount", DataTypes.StringType)
                .add(" total_amount", DataTypes.StringType);
    }

    public static Dataset<Row> applySchema(Dataset<Row> dataset) {
        dataset = dataset.withColumn("medallion", dataset.col("medallion").cast(DataTypes.IntegerType));
        dataset = dataset.withColumn(" hack_license", dataset.col(" hack_license").cast(DataTypes.IntegerType));
        dataset = dataset.withColumn(" vendor_id", dataset.col(" vendor_id").cast(DataTypes.IntegerType));
        dataset = dataset.withColumn(" pickup_datetime", dataset.col(" pickup_datetime").cast(DataTypes.TimestampType));
        dataset = dataset.withColumn(" rate_code", dataset.col(" rate_code").cast(DataTypes.IntegerType));
        dataset = dataset.withColumn(" dropoff_datetime", dataset.col(" dropoff_datetime").cast(DataTypes.TimestampType));
        dataset = dataset.withColumn(" passenger_count", dataset.col(" passenger_count").cast(DataTypes.IntegerType));
        dataset = dataset.withColumn(" trip_time_in_secs", dataset.col(" trip_time_in_secs").cast(DataTypes.IntegerType));
        dataset = dataset.withColumn(" trip_distance", dataset.col(" trip_distance").cast(DataTypes.IntegerType));
        dataset = dataset.withColumn(" pickup_longitude", dataset.col(" pickup_longitude").cast(DataTypes.DoubleType));
        dataset = dataset.withColumn(" pickup_latitude", dataset.col(" pickup_latitude").cast(DataTypes.DoubleType));
        dataset = dataset.withColumn(" dropoff_longitude", dataset.col(" dropoff_longitude").cast(DataTypes.DoubleType));
        dataset = dataset.withColumn(" dropoff_latitude", dataset.col(" dropoff_latitude").cast(DataTypes.DoubleType));
        dataset = dataset.withColumn(" fare_amount", dataset.col(" fare_amount").cast(DataTypes.DoubleType));
        dataset = dataset.withColumn(" surcharge", dataset.col(" surcharge").cast(DataTypes.DoubleType));
        dataset = dataset.withColumn(" mta_tax", dataset.col(" mta_tax").cast(DataTypes.DoubleType));
        dataset = dataset.withColumn(" tip_amount", dataset.col(" tip_amount").cast(DataTypes.DoubleType));
        dataset = dataset.withColumn(" tolls_amount", dataset.col(" tolls_amount").cast(DataTypes.DoubleType));
        dataset = dataset.withColumn(" total_amount", dataset.col(" total_amount").cast(DataTypes.DoubleType));

        return dataset;
    }

    public static List getColumnNames() {
        List<String> list = new ArrayList<>();
        list.add("medallion");
        list.add(" hack_license");
        list.add(" vendor_id");
        list.add(" pickup_datetime");
        list.add(" rate_code");
        list.add(" store_and_fwd_flag");
        list.add(" dropoff_datetime");
        list.add(" passenger_count");
        list.add(" trip_time_in_secs");
        list.add(" trip_distance");
        list.add(" pickup_longitude");
        list.add(" pickup_latitude");
        list.add(" dropoff_longitude");
        list.add(" dropoff_latitude");
        list.add(" payment_type");
        list.add(" fare_amount");
        list.add(" surcharge");
        list.add(" mta_tax");
        list.add(" tip_amount");
        list.add(" tolls_amount");
        list.add(" total_amount");

        return list;
    }

}
