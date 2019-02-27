package org.infai.senergy.benchmark.smartmeter.util;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;
import org.infai.senergy.benchmark.util.Welford;

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
    public Iterator<RowWithDiff> call(String key, Iterator<Row> values, GroupState<GroupStateContainer> groupState) {
        List<RowWithDiff> rowsWithDiff = new ArrayList<>();
        Double oldValue, diff, newValue, avg, stddev;
        Timestamp oldTime, newTime;
        RowWithDiff newRowWithDiff;
        Welford welford;
        GroupStateContainer state;

        Row row;
        while(values.hasNext()) {
            row = values.next();
            newValue = row.getDouble(row.fieldIndex("CONSUMPTION"));
            newTime = row.getTimestamp(row.fieldIndex("TIMESTAMP_UTC"));
            if (groupState.exists()) {
                state = groupState.get();
                oldValue = state.getValue();
                oldTime = state.getTime();
                welford = state.getWelford();

                long timeDiff;
                if ((timeDiff = newTime.getTime() - oldTime.getTime()) <= 0) {
                    continue; //Out of order or same timestamp. Skip this value and won't take it into consideration.
                }
                diff = (newValue - oldValue) / ((double) timeDiff);
                welford.update(diff);
                avg = welford.mean();
                stddev = welford.std();
            }else{
                avg = 0.0;
                stddev = 0.0;
                diff = 0.0;
                state = new GroupStateContainer();
                welford = new Welford();
                state.setWelford(welford);
            }
            newRowWithDiff = new RowWithDiff();
            newRowWithDiff.setCONSUMPTION(newValue);
            newRowWithDiff.setDIFF(diff);
            newRowWithDiff.setMETER_ID(key);
            newRowWithDiff.setTIMESTAMP_UTC(newTime);
            newRowWithDiff.setSEGMENT(row.getString(row.fieldIndex("SEGMENT")));
            newRowWithDiff.setAVG_DIFF(avg);
            newRowWithDiff.setSTDDEV_DIFF(stddev);

            rowsWithDiff.add(newRowWithDiff);
            state.setTime(newTime);
            state.setValue(newValue);

            groupState.update(state);
        }
        return rowsWithDiff.iterator();
    }


}
