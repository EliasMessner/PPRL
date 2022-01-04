/**
 * This class should be used instead of PrecisionRecallStats if not all Records will be evaluated, e.g. due to blocking.
 * The non-evaluated Records are automatically assumed to be classified as non-matches.
 * The amount of true matches must be known beforehand, as opposed to the parent class PrecisionRecallStats.
 */
public class PrecisionRecallStatsForBlocking extends PrecisionRecallStats {

    private final long totalMatches;
    private final long totalNonMatches;

    public PrecisionRecallStatsForBlocking (long totalSize, long totalMatches) {
        super();
        this.totalMatches = totalMatches;
        this.totalNonMatches = totalSize - totalMatches;
    }

    @Override
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
}
