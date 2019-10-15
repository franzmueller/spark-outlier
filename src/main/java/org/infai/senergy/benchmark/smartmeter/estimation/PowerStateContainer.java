package org.infai.senergy.benchmark.smartmeter.estimation;


import com.yahoo.labs.samoa.instances.InstancesHeader;
import moa.classifiers.Classifier;

import java.io.Serializable;

public class PowerStateContainer implements Serializable {
    protected InstancesHeader header;
    protected Classifier classifier;
    protected int numTrained;

    public Classifier getClassifier() {
        return classifier;
    }

    public void setClassifier(Classifier classifier) {
        this.classifier = classifier;
    }

    public InstancesHeader getHeader() {
        return header;
    }

    public void setHeader(InstancesHeader header) {
        this.header = header;
    }

    public int getNumTrained() {
        return numTrained;
    }

    public void setNumTrained(int numTrained) {
        this.numTrained = numTrained;
    }
}
