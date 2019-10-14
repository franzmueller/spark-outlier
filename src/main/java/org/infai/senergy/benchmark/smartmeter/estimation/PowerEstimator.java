package org.infai.senergy.benchmark.smartmeter.estimation;

import com.yahoo.labs.samoa.instances.*;
import moa.classifiers.Classifier;
import moa.classifiers.functions.AdaGrad;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PowerEstimator implements FlatMapGroupsWithStateFunction<String, Row, PowerStateContainer, RowWithEstimation> {

    @Override
    public Iterator<RowWithEstimation> call(String key, Iterator<Row> iterator, GroupState<PowerStateContainer> groupState) {
        List<RowWithEstimation> rowsWithEstimation = new ArrayList<>();
        PowerStateContainer state;

        if (groupState.exists()) {
            state = groupState.get();
        } else {
            //=> First run for this METER_ID, prepare everything
            state = new PowerStateContainer();
            ArrayList<Attribute> attributesList = new ArrayList<>();
            attributesList.add(new Attribute("timestamp"));
            attributesList.add(new Attribute("value"));

            Instances instances = new Instances(key, attributesList, 0);
            instances.setClassIndex(1);
            InstancesHeader header = new InstancesHeader(instances);
            state.setHeader(header);

            Classifier classifier = new AdaGrad();
            state.setClassifier(classifier);
        }

        while (iterator.hasNext()) {
            //Get data
            Row row = iterator.next();
            double value = row.getDouble(row.fieldIndex("CONSUMPTION"));
            Timestamp TIMESTAMP_UTC = row.getTimestamp(row.fieldIndex("TIMESTAMP_UTC"));
            long timestampMillis = TIMESTAMP_UTC.getTime();

            //Prepare and train instance
            Instance instance = new DenseInstance(2);
            instance.setDataset(state.getHeader());
            instance.setValue(0, timestampMillis);
            instance.setValue(1, value);
            state.getClassifier().trainOnInstance(instance);

            //Regression
            Instance eoy = new DenseInstance(2);
            double tsEOY = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestampMillis), ZoneOffset.UTC)
                    .withDayOfYear(1).withHour(0).withMinute(0).withSecond(0).withNano(0).plusYears(1).minusSeconds(1)
                    .toInstant().toEpochMilli();
            eoy.setValue(0, tsEOY);
            eoy.setDataset(state.getHeader());
            double PREDICTION = state.getClassifier().getPredictionForInstance(eoy).getVotes()[0];

            //Create output
            RowWithEstimation rowWithEstimation = new RowWithEstimation();
            rowWithEstimation.setCONSUMPTION(value);
            rowWithEstimation.setMETER_ID(key);
            rowWithEstimation.setTIMESTAMP_UTC(TIMESTAMP_UTC);
            rowWithEstimation.setSEGMENT(row.getString(row.fieldIndex("SEGMENT")));
            rowWithEstimation.setPREDICTION(PREDICTION);
            rowWithEstimation.setPREDICTION_TIMESTAMP(new Timestamp((long) tsEOY));
            rowsWithEstimation.add(rowWithEstimation);
        }
        //Wrap it up
        groupState.update(state);
        return rowsWithEstimation.iterator();
    }
}
