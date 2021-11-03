package com.company;

import java.util.Map;

public class PrecisionRecallStats {

    public int tp = 0, tn = 0, fp = 0, fn = 0;

    public <K> PrecisionRecallStats(Map<K, Boolean> trueMatches, Map<K, Boolean> predictedMatches) {
        assert trueMatches.size() == predictedMatches.size();
        for (K key : trueMatches.keySet()) {
            if (trueMatches.get(key) && predictedMatches.get(key))
                tp++;
            else if (trueMatches.get(key) && !predictedMatches.get(key))
                fn++;
            else if (!trueMatches.get(key) && predictedMatches.get(key))
                fp++;
            else if (!trueMatches.get(key) && !predictedMatches.get(key))
                tn++;
        }
    }

    public double getPrecision() {
        return 1.0 * tp/(tp + fp);
    }

    public double getRecall() {
        return 1.0 * tp/(tp + fn);
    }

    @Override
    public String toString() {
        return "tp: " + tp + "\ntn: " + tn + "\nfp: " + fp + "\nfn: " + fn + "\nprecision: " + getPrecision()
                + "\nrecall: " + getRecall();
    }
}
