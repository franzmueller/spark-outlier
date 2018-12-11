package org.infai.senergy.benchmark.smartmeter;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.ParamPair;
import org.apache.spark.ml.util.Identifiable$;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.infai.senergy.benchmark.smartmeter.util.DateValuePair;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DiffTransformer extends Transformer{
    private String uid;
    private Map<String, DateValuePair> oldValues;

    public DiffTransformer(){
        uid = Identifiable$.MODULE$.randomUID("DiffTransformer");
        oldValues = new HashMap<>();
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        List<?> list = dataset.collectAsList();
        int meterIndex = (int) dataset.schema().getFieldIndex("METER_ID").get(); //Possible point of failure
        int consIndex = (int) dataset.schema().getFieldIndex("CONSUMPTION").get(); //Possible point of failure
        int timeIndex = (int)  dataset.schema().getFieldIndex("TIMESTAMP_UTC").get(); //Possible point of failure

        List<Double> diffs = new ArrayList();
        int i = 0;
        for(Object element: list){
            Row row = (Row) element; //Lets hope this works everytime
            String meterID = row.getString(meterIndex);
            double consumption = row.getDouble(consIndex);
            Timestamp timestamp = row.getTimestamp(timeIndex);
            if(oldValues.containsKey(meterID)){
                if(oldValues.get(meterID).getTime().before(timestamp)) {
                    diffs.add(consumption - (double) oldValues.get(meterID).getValue());
                    oldValues.put(meterID, new DateValuePair(timestamp, consumption));
                }else{
                    diffs.add(0.0); //TODO is this good?
                }
            }else{
                oldValues.put(meterID, new DateValuePair(timestamp, consumption));
                diffs.add(0.0); //TODO is this good?
            }

            i++;
        }
        //TODO create row from diffs and add as column
        return null; //TODO
    }

    @Override
    public StructType transformSchema(StructType structType) {
        return structType.add("DIFF", DataTypes.DoubleType);
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        DiffTransformer dt = defaultCopy(paramMap);
        dt.uid = uid;
        for(ParamPair pp :paramMap.toList())
            dt.set(pp);
        //TODO copy oldBValues over to dt
        /*
            TODO: copy all local values
         */
        return dt;
    }

    @Override
    public String uid() {
        return uid;
    }
}
