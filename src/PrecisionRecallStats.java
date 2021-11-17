import java.math.BigInteger;
import java.util.Map;

/**
 * Class for evaluating precision and recall by given predictions and true values assigned
 */
public class PrecisionRecallStats {

    public BigInteger tp, tn, fp, fn;

    public PrecisionRecallStats() {
        reset();
    }

    public void evaluate(Boolean truth, Boolean prediction) {
        if (truth && prediction)
            tp = tp.add(BigInteger.ONE);
        else if (!truth && !prediction) {
            tn = tn.add(BigInteger.ONE);
        } else if (truth) {
            fn = fn.add(BigInteger.ONE);
        } else {
            fp = fp.add(BigInteger.ONE);
        }
    }

    public void reset() {
        tp = BigInteger.ZERO;
        tn = BigInteger.ZERO;
        fp = BigInteger.ZERO;
        fn = BigInteger.ZERO;
    }

    public double getPrecision() {
        return tp.divide(tp.add(fp)).doubleValue();
    }

    public double getRecall() {
        return tp.divide(tp.add(fn)).doubleValue();
    }

    @Override
    public String toString() {
        return "tp: " + tp + "\ntn: " + tn + "\nfp: " + fp + "\nfn: " + fn + "\nprecision: " + getPrecision()
                + "\nrecall: " + getRecall();
    }
}
