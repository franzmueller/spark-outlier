package org.infai.senergy.benchmark.smartmeter.util;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class FlatDiff implements FlatMapGroupsWithStateFunction<String,Row, TimestampDoublePair,RowWithDiff> {


    public FlatDiff(){
        super();
    }

    //Update function
    @Override
    public Iterator<RowWithDiff> call(String key, Iterator<Row> values, GroupState<TimestampDoublePair> state) {
        List<RowWithDiff> rowsWithDiff = new ArrayList<>();
        Double oldValue, diff, newValue;
        Timestamp oldTime, newTime;
        TimestampDoublePair newState;
        RowWithDiff newRowWithDiff;

        Row row;
        while(values.hasNext()) {
            row = values.next();
            newValue = row.getDouble(row.fieldIndex("CONSUMPTION"));
            newTime = row.getTimestamp(row.fieldIndex("TIMESTAMP_UTC"));
            if(state.exists()) {
                oldValue = state.get().getValue();
                oldTime = state.get().getTime();

                long timeDiff;
                if ((timeDiff = newTime.getTime() - oldTime.getTime()) <= 0) {
                    continue; //Out of order or same timestamp. Skip this value and won't take it into consideration.
                }
                diff = (newValue - oldValue) / ((double) timeDiff);
            }else{
                diff = 0.0;
            }
            newRowWithDiff = new RowWithDiff();
            newRowWithDiff.setCONSUMPTION(newValue);
            newRowWithDiff.setDIFF(diff);
            newRowWithDiff.setMETER_ID(key);
            newRowWithDiff.setTIMESTAMP_UTC(newTime);
            newRowWithDiff.setSEGMENT(row.getString(row.fieldIndex("SEGMENT")));

            rowsWithDiff.add(newRowWithDiff);
            newState = new TimestampDoublePair();
            newState.setTime(newTime);
            newState.setValue(newValue);
            state.update(newState);
        }
        return rowsWithDiff.iterator();
    }


}
