package org.infai.senergy.benchmark.smartmeter.util;

import java.io.Serializable;

public class TimeRow implements Serializable {
    private String METER_ID;
    private int[] timeCounts;

    public String getMETER_ID() {
        return METER_ID;
    }

    public void setMETER_ID(String METER_ID) {
        this.METER_ID = METER_ID;
    }

    public int[] getTimeCounts() {
        return timeCounts;
    }

    public void setTimeCounts(int[] timeCounts) {
        this.timeCounts = timeCounts;
    }
}
