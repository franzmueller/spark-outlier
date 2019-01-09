package org.infai.senergy.benchmark.smartmeter.util;

import java.io.Serializable;
import java.sql.Timestamp;

public class RowWithDiff implements Serializable {
    protected String METER_ID;
    protected String SEGMENT;
    protected double CONSUMPTION;
    protected double DIFF;
    protected double AVG_DIFF;
    protected double STDDEV_DIFF;
    protected Timestamp TIMESTAMP_UTC;

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

    public double getDIFF() {
        return DIFF;
    }

    public void setDIFF(double DIFF) {
        this.DIFF = DIFF;
    }

    public Timestamp getTIMESTAMP_UTC() {
        return TIMESTAMP_UTC;
    }

    public void setTIMESTAMP_UTC(Timestamp TIMESTAMP_UTC) {
        this.TIMESTAMP_UTC = TIMESTAMP_UTC;
    }

    public double getAVG_DIFF() {
        return AVG_DIFF;
    }

    public void setAVG_DIFF(double AVG_DIFF) {
        this.AVG_DIFF = AVG_DIFF;
    }

    public double getSTDDEV_DIFF() {
        return STDDEV_DIFF;
    }

    public void setSTDDEV_DIFF(double STDDEV_DIFF) {
        this.STDDEV_DIFF = STDDEV_DIFF;
    }
}
