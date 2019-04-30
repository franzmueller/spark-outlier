package org.infai.senergy.benchmark.smartmeter.util;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlatTimeanalysis implements FlatMapGroupsWithStateFunction<String, Row, TimeCounter, TimeRow> {
    @Override
    public Iterator<TimeRow> call(String s, Iterator<Row> values, GroupState<TimeCounter> groupState) throws Exception {
        List<TimeRow> timeRows = new ArrayList<>();

        Timestamp time;
        TimeCounter state;
        Row row;
        TimeRow timeRow;
        int[] timeCounts;
        while (values.hasNext()) {
            row = values.next();
            time = row.getTimestamp(row.fieldIndex("TIMESTAMP_UTC"));
            if (groupState.exists()) {
                state = groupState.get();
                timeCounts = state.getTimeCounts();
            } else {
                state = new TimeCounter();
                timeCounts = new int[24];
            }
            long hourOfDay = (time.getTime() % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60); //millies mod millies-per-day div millies-per-hour
            timeCounts[(int) hourOfDay]++;
            state.setTimeCounts(timeCounts);
            groupState.update(state);

            timeRow = new TimeRow();
            timeRow.setMETER_ID(s);
            timeRow.setOne(timeCounts[0]);
            timeRow.setTwo(timeCounts[1]);
            timeRow.setThree(timeCounts[2]);
            timeRow.setFour(timeCounts[3]);
            timeRow.setFive(timeCounts[4]);
            timeRow.setSix(timeCounts[5]);
            timeRow.setSeven(timeCounts[6]);
            timeRow.setEight(timeCounts[7]);
            timeRow.setNine(timeCounts[8]);
            timeRow.setTen(timeCounts[9]);
            timeRow.setEleven(timeCounts[10]);
            timeRow.setTwelfe(timeCounts[11]);
            timeRow.setThirteen(timeCounts[12]);
            timeRow.setFourteen(timeCounts[13]);
            timeRow.setFiveteen(timeCounts[14]);
            timeRow.setSixteen(timeCounts[15]);
            timeRow.setSeventeen(timeCounts[16]);
            timeRow.setEighteen(timeCounts[17]);
            timeRow.setNineteen(timeCounts[18]);
            timeRow.setTwenty(timeCounts[19]);
            timeRow.setTwentyOne(timeCounts[20]);
            timeRow.setTwentytwo(timeCounts[21]);
            timeRow.setTwentythree(timeCounts[22]);
            timeRow.setTwentyfour(timeCounts[23]);

            timeRows.add(timeRow);
        }
        return timeRows.iterator();
    }
}
