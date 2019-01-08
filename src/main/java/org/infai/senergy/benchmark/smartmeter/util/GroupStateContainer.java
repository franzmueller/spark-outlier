package org.infai.senergy.benchmark.smartmeter.util;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

public class GroupStateContainer implements Serializable {
    protected Double value = 0.0;
    protected Timestamp time;
    protected List<Double> diffs;

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }

    public List<Double> getDiffs() {
        return diffs;
    }

    public void setDiffs(List<Double> diffs) {
        this.diffs = diffs;
    }
}
