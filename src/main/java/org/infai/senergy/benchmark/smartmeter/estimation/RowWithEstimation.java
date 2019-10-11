package org.infai.senergy.benchmark.smartmeter.estimation;

import java.io.Serializable;
import java.sql.Timestamp;

public class RowWithEstimation implements Serializable {
    protected String METER_ID;
    protected String SEGMENT;
    protected double CONSUMPTION;
    protected Timestamp TIMESTAMP_UTC;
    protected Timestamp PREDICTION_TIMESTAMP;
    protected double PREDICTION;

    public String getMETER_ID() {
        return METER_ID;
    }

    public void setMETER_ID(String METER_ID) {
        this.METER_ID = METER_ID;
    }

    public String getSEGMENT() {
        return SEGMENT;
    }

    public void setSEGMENT(String SEGMENT) {
        this.SEGMENT = SEGMENT;
    }

    public double getCONSUMPTION() {
        return CONSUMPTION;
    }

    public void setCONSUMPTION(double CONSUMPTION) {
        this.CONSUMPTION = CONSUMPTION;
    }

    public Timestamp getTIMESTAMP_UTC() {
        return TIMESTAMP_UTC;
    }

    public void setTIMESTAMP_UTC(Timestamp TIMESTAMP_UTC) {
        this.TIMESTAMP_UTC = TIMESTAMP_UTC;
    }

    public Timestamp getPREDICTION_TIMESTAMP() {
        return PREDICTION_TIMESTAMP;
    }

    public void setPREDICTION_TIMESTAMP(Timestamp PREDICTION_TIMESTAMP) {
        this.PREDICTION_TIMESTAMP = PREDICTION_TIMESTAMP;
    }

    public double getPREDICTION() {
        return PREDICTION;
    }

    public void setPREDICTION(double PREDICTION) {
        this.PREDICTION = PREDICTION;
    }
}
