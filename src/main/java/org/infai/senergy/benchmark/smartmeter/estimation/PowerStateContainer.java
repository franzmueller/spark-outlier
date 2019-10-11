package org.infai.senergy.benchmark.smartmeter.estimation;

import weka.classifiers.Classifier;
import weka.core.Instances;

import java.io.Serializable;

public class PowerStateContainer implements Serializable {
    protected Classifier classifier;
    protected Instances instances;

    public Classifier getClassifier() {
        return classifier;
    }

    public void setClassifier(Classifier classifier) {
        this.classifier = classifier;
    }

    public Instances getInstances() {
        return instances;
    }

    public void setInstances(Instances instances) {
        this.instances = instances;
    }
}
