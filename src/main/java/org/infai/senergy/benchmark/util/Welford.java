package org.infai.senergy.benchmark.util;//Based on https://gist.github.com/alexalemi/2151722

public class Welford {

    private double k = 0, M = 0, S = 0;

    public void update(double value) {
        k++;
        double newM = M + (value - M) * 1.0 / k;
        double newS = S + (value - M) * (value - newM);
        M = newM;
        S = newS;
    }

    public double mean() {
        return M;
    }

    public double std() {
        if (k == 1) {
            return 0;
        }
        return Math.sqrt(S / (k - 1));
    }
}
