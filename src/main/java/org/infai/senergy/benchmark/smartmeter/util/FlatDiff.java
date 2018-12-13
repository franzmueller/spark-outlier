package org.infai.senergy.benchmark.smartmeter.util;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;
import org.infai.senergy.benchmark.smartmeter.util.TimestampDoublePair;

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
        Double oldValue = 0.0, diff = 0.0, newValue;
        Timestamp oldTime, newTime;
        TimestampDoublePair newState;
        RowWithDiff newRowWithDiff;

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
            if((timeDiff = newTime.compareTo(oldTime)) <= 0)
                continue; //Out of order. Will skip this value and won't take it into consideration

            diff = (newValue - oldValue) / ((double) timeDiff);

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
