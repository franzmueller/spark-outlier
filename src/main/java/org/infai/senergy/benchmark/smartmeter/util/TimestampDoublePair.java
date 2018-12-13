package org.infai.senergy.benchmark.smartmeter.util;

import java.io.Serializable;
import java.sql.Timestamp;

public class TimestampDoublePair implements Serializable {
    protected Double value=0.0;
    protected Timestamp time;

    public Double getValue() {
        return value;
    }

    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}
