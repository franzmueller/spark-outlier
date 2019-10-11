package org.infai.senergy.benchmark.smartmeter.estimation;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;
import weka.classifiers.Classifier;
import weka.classifiers.functions.SMOreg;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PowerEstimator implements FlatMapGroupsWithStateFunction<String, Row, PowerStateContainer, RowWithEstimation> {

    @Override
    public Iterator<RowWithEstimation> call(String key, Iterator<Row> iterator, GroupState<PowerStateContainer> groupState) {
        List<RowWithEstimation> rowsWithEstimation = new ArrayList<>();
        PowerStateContainer state;
        Instances instances;
        Classifier classifier = new SMOreg();
        RowWithEstimation rowWithEstimation = new RowWithEstimation();


        if (groupState.exists()) {
            state = groupState.get();
            instances = state.getInstances();
        } else {
            state = new PowerStateContainer();
            ArrayList<Attribute> attributesList = new ArrayList<>();
            attributesList.add(new Attribute("timestamp"));
            attributesList.add(new Attribute("value"));
            instances = new Instances("", attributesList, 1);
            instances.setClassIndex(1);
        }

        while (iterator.hasNext()) {
            Row row = iterator.next();
            double value = row.getDouble(row.fieldIndex("CONSUMPTION"));
            Timestamp TIMESTAMP_UTC = row.getTimestamp(row.fieldIndex("TIMESTAMP_UTC"));
            double timestampMillis = TIMESTAMP_UTC.getTime();

            Instance instance = new DenseInstance(2);
            instance.setDataset(instances);
            instance.setValue(0, timestampMillis);
            instance.setValue(1, value);
            instances.add(instance);

            rowWithEstimation.setCONSUMPTION(value);
            rowWithEstimation.setMETER_ID(key);
            rowWithEstimation.setTIMESTAMP_UTC(TIMESTAMP_UTC);
            rowWithEstimation.setSEGMENT(row.getString(row.fieldIndex("SEGMENT")));
        }

        Instance eoy = new DenseInstance(1);
        double tsEOY = 1577836799000.0; //2019-12-31 23:59:59 TODO:config
        eoy.setValue(0, tsEOY);

        try {
            classifier.buildClassifier(instances);
            double PREDICTION = classifier.classifyInstance(eoy);
            rowWithEstimation.setPREDICTION(PREDICTION);
            rowWithEstimation.setPREDICTION_TIMESTAMP(new Timestamp((long) tsEOY));
        } catch (Exception e) {
            e.printStackTrace();
        }

        rowsWithEstimation.add(rowWithEstimation);

        state.setInstances(instances);
        groupState.update(state);
        return rowsWithEstimation.iterator();
    }
}
