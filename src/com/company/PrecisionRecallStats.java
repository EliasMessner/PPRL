package com.company;

import java.util.Map;

/**
 * Class for evaluating precision and recall by given predictions and true values assigned
 */
public class PrecisionRecallStats {

    public int tp = 0, tn = 0, fp = 0, fn = 0;

    /**
     * When an instance is created, the tp, tn, fp, fn are counted.
     * @param trueMatches a map containing the dataset identifiers as keys and truth as values.
     * @param predictedMatches a map containing the same dataset identifiers as trueMatches as keys, and predictions
     *                         as values.
     * @param <K>
     */
    public <K> PrecisionRecallStats(Map<K, Boolean> trueMatches, Map<K, Boolean> predictedMatches) {
        assert trueMatches.size() == predictedMatches.size();
        trueMatches.keySet().parallelStream().forEach(key -> {
            if (trueMatches.get(key) && predictedMatches.get(key))
                tp++;
            else if (trueMatches.get(key) && !predictedMatches.get(key))
                fn++;
            else if (!trueMatches.get(key) && predictedMatches.get(key))
                fp++;
            else if (!trueMatches.get(key) && !predictedMatches.get(key))
                tn++;
        });
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
