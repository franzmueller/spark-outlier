package org.infai.senergy.benchmark.smartmeter.util;

import java.io.Serializable;

public class TimeCounter implements Serializable {
    protected int[] timeCounts = new int[24];

    public int[] getTimeCounts() {
        return timeCounts;
    }

    public void setTimeCounts(int[] timeCounts) {
        this.timeCounts = timeCounts;
    }
}
