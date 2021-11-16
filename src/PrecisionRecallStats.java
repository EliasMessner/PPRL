import java.util.Map;

/**
 * Class for evaluating precision and recall by given predictions and true values assigned
 */
public class PrecisionRecallStats {

    public int tp, tn, fp, fn;

    public PrecisionRecallStats() {
        reset();
    }

    public void evaluate(Boolean truth, Boolean prediction) {
        if (truth && prediction)
            tp++;
        else if (!truth && !prediction) {
            tn++;
        } else if (truth) {
            fn++;
        } else {
            fp++;
        }
    }

    public void reset() {
        tp = 0;
        tn = 0;
        fp = 0;
        fn = 0;
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
