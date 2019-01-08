package org.infai.senergy.benchmark.smartmeter.util;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class FlatDiff implements FlatMapGroupsWithStateFunction<String, Row, GroupStateContainer, RowWithDiff> {


    public FlatDiff(){
        super();
    }

    //Update function
    @Override
    public Iterator<RowWithDiff> call(String key, Iterator<Row> values, GroupState<GroupStateContainer> state) {
        List<RowWithDiff> rowsWithDiff = new ArrayList<>();
        Double oldValue, diff, newValue, avg, stddev;
        Timestamp oldTime, newTime;
        GroupStateContainer newState;
        RowWithDiff newRowWithDiff;
        List<Double> diffs;

        Row row;
        while(values.hasNext()) {
            row = values.next();
            newValue = row.getDouble(row.fieldIndex("CONSUMPTION"));
            newTime = row.getTimestamp(row.fieldIndex("TIMESTAMP_UTC"));
            if(state.exists()) {
                oldValue = state.get().getValue();
                oldTime = state.get().getTime();
                diffs = state.get().getDiffs();

                long timeDiff;
                if ((timeDiff = newTime.getTime() - oldTime.getTime()) <= 0) {
                    continue; //Out of order or same timestamp. Skip this value and won't take it into consideration.
                }
                diff = (newValue - oldValue) / ((double) timeDiff);
                diffs.add(diff);

                //Compute average
                double sum = 0.0;
                for (Double d : diffs) {
                    sum += d;
                }
                avg = sum / diffs.size();

                //Compute stddev
                sum = 0.0;
                for (Double d : diffs) {
                    sum += (d - avg) * (d - avg);
                }
                sum = sum / diffs.size();
                stddev = Math.sqrt(sum);
            }else{
                diff = 0.0;
                diffs = new ArrayList<>();
                diffs.add(diff);
                avg = diff;
                stddev = 0.0;
            }
            newRowWithDiff = new RowWithDiff();
            newRowWithDiff.setCONSUMPTION(newValue);
            newRowWithDiff.setDIFF(diff);
            newRowWithDiff.setMETER_ID(key);
            newRowWithDiff.setTIMESTAMP_UTC(newTime);
            newRowWithDiff.setSEGMENT(row.getString(row.fieldIndex("SEGMENT")));
            newRowWithDiff.setAVG_DIFF(avg);
            newRowWithDiff.setSTDDEV_DIFF(stddev);
            //TODO add avg and stddev

            rowsWithDiff.add(newRowWithDiff);
            newState = new GroupStateContainer();
            newState.setTime(newTime);
            newState.setValue(newValue);
            newState.setDiffs(diffs);
            state.update(newState);
        }
        return rowsWithDiff.iterator();
    }


}
