import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Threadsafe class for evaluating precision and recall by given predictions and true values assigned
 */
public class PrecisionRecallStats {

    public AtomicLong tp, tn, fp, fn;
    private final long totalMatches;
    private final long totalNonMatches;

    public PrecisionRecallStats(long totalSize, long totalMatches) {
        tp = new AtomicLong();
        tn = new AtomicLong();
        fp = new AtomicLong();
        fn = new AtomicLong();
        this.totalMatches = totalMatches;
        this.totalNonMatches = totalSize - totalMatches;
    }

    public PrecisionRecallStats(long tp, long tn, long fp, long fn, long totalSize, long totalMatches) {
        this.tp = new AtomicLong(tp);
        this.tn = new AtomicLong(tn);
        this.fp = new AtomicLong(fp);
        this.fn = new AtomicLong(fn);
        this.totalMatches = totalMatches;
        this.totalNonMatches = totalSize - totalMatches;
    }

    public long getTp() {
        return tp.get();
    }

    public void setTp(long tp) {
        this.tp.set(tp);
    }

    public long getTn() {
        return tn.get();
    }

    public void setTn(long tn) {
        this.tn.set(tn);
    }

    public long getFp() {
        return fp.get();
    }

    public void setFp(long fp) {
        this.fp.set(fp);
    }

    public long getFn() {
        return fn.get();
    }

    public void setFn(long fn) {
        this.fn.set(fn);
    }

    public void evaluateAll(Set<PersonPair> linking) {
        for (PersonPair personPair : linking) {
            evaluate(personPair.getA().equalGlobalID(personPair.getB()), true);
        }
    }

    public void evaluate(Boolean truth, Boolean prediction) {
        if (prediction) {
            if (truth) {
                tp.incrementAndGet();
            } else {
                fp.incrementAndGet();
            }
        }
        tn.set(totalNonMatches - fp.get());
        fn.set(totalMatches - tp.get());
    }

    public void reset() {
        tp.set(0);
        tn.set(0);
        fp.set(0);
        fn.set(0);
    }

    public double getPrecision() {
        // tp / (tp + fp)
        return 1.0 * tp.get() / (tp.get() + fp.get());
    }

    public double getRecall() {
        // tp / (tp + fn)
        return 1.0 * tp.get() / (tp.get() + fn.get());
    }

    public double getF1Score() {
        return 2.0 * ((getPrecision() * getRecall())/(getPrecision() + getRecall()));
    }

    @Override
    public String toString() {
        return "tp: " + tp + "\ntn: " + tn + "\nfp: " + fp + "\nfn: " + fn + "\nprecision: " + getPrecision()
                + "\nrecall: " + getRecall();
    }
}
