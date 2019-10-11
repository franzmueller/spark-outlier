package org.infai.senergy.benchmark.smartmeter.estimation;

import weka.core.Instances;

import java.io.Serializable;

public class PowerStateContainer implements Serializable {
    protected Instances instances;

    public Instances getInstances() {
        return instances;
    }

    public void setInstances(Instances instances) {
        this.instances = instances;
    }
}
