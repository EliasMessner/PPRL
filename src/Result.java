/**
 * Contains parameters and resulting stats of a program iteration
 */
public record Result(Parameters parameters, PrecisionRecallStats precisionRecallStats) {

    public String toCSVString() {
        return parameters.mode().toString() + ","
                + parameters.blocking() + ","
                + parameters.weightedAttributes() + ","
                + parameters.tokenSalting() + ","
                + parameters.l() + ","
                + parameters.k() + ","
                + parameters.t() + ","
                + precisionRecallStats.tp + ","
                + precisionRecallStats.tn + ","
                + precisionRecallStats.fp + ","
                + precisionRecallStats.fn + ","
                + precisionRecallStats.getPrecision() + ","
                + precisionRecallStats.getRecall() + ","
                + precisionRecallStats.getF1Score();
    }

    public static String getCSVHeadLine() {
        return "mode,b,wa,ps,l,k,t,tp,tn,fp,fn,precision,recall,f1-score";
    }
}
