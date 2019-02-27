package org.infai.senergy.benchmark.smartmeter.util;

import org.infai.senergy.benchmark.util.Welford;

import java.io.Serializable;
import java.sql.Timestamp;

public class GroupStateContainer implements Serializable {
    protected Double value = 0.0;
    protected Timestamp time;
    protected Welford welford;

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

    public Welford getWelford() {
        return welford;
    }

    public void setWelford(Welford welford) {
        this.welford = welford;
    }
}
