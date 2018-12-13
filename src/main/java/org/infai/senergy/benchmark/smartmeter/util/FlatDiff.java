package org.infai.senergy.benchmark.smartmeter.util;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;
import org.infai.senergy.benchmark.smartmeter.util.TimestampDoublePair;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class FlatDiff implements FlatMapGroupsWithStateFunction<String,Row, TimestampDoublePair,Double> {


    public FlatDiff(){
        super();
    }

    //Update function
    @Override
    public Iterator<Double> call(String key, Iterator<Row> values, GroupState<TimestampDoublePair> state) {
        List<Double> diffs = new ArrayList<>();
        Double oldValue = 0.0, diff = 0.0, newValue;
        Timestamp oldTime, newTime;
        TimestampDoublePair newState;

        Row row;
        while(values.hasNext()) {
            row = values.next();
            if(state.exists()) {
                oldValue = state.get().getValue();
                oldTime = state.get().getTime();
            }else{
                //Old=New --> Diff=0
                oldValue  = row.getDouble(row.fieldIndex("CONSUMPTION"));
                oldTime = row.getTimestamp(row.fieldIndex("TIMESTAMP_UTC"));
            }
            newValue = row.getDouble(row.fieldIndex("CONSUMPTION"));
            newTime = row.getTimestamp(row.fieldIndex("TIMESTAMP_UTC"));

            int timeDiff;
            if((timeDiff = newTime.compareTo(oldTime)) > 0) {
                diff = (newValue - oldValue) / ((double) timeDiff);
            }else{
                diff = 0.0; //Out of order or same timestamp TODO better solution
            }

            diffs.add(diff);
            newState = new TimestampDoublePair();
            newState.setTime(newTime);
            newState.setValue(newValue);
            state.update(newState);
        }

        return diffs.iterator();
    }


}
