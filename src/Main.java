import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Main {

    /**
     * The Main method. Arguments should be threshold, hashAreaSize, hashFunctionCount, subsetSize, parallelFlag
     * The subsetSize is the size of the subset to be evaluated, e.g. the first 500 entries of each dataset
     * source. If set to -1, the full dataset will be evaluated.
     * parallelFlag should be "p" or "s" to state whether the program should be executed in parallel or serial mode.
     * @param args argument vector containing threshold, hashAreaSize, hashFunctionCount, (optional) subsetSize.
     */
    public static void main(String[] args) {
        double threshold = Double.parseDouble(args[0]);
        int hashAreaSize = Integer.parseInt(args[1]);
        int hashFunctionCount = Integer.parseInt(args[2]);
        int subsetSize = Integer.parseInt(args[3]);
        boolean parallel = args[4].equals("p");
        try {
            long startTime = System.nanoTime();
            // parse the data from the file
            System.out.println("Parsing Data...");
            Person[] dataSet = DataHandler.parseData("datasets/2021_NCVR_Panse_001/dataset_ncvr_dirty.csv", 200000);
            Person[][] splitData = DataHandler.splitDataBySource(dataSet);
            Person[] A = splitData[0];
            Person[] B = splitData[1];

            if (subsetSize > 0) {
                A = Arrays.copyOfRange(A, 0, subsetSize);
                B = Arrays.copyOfRange(B, 0, subsetSize);
            }

            Map<Person[], Boolean> trueMatches = new HashMap<>(); // key should be two Person Objects
            Map<Person[], Boolean> predictedMatches = new HashMap<>();

            // create the bloom filters
            System.out.println("Creating Bloom filters...");
            ProgressHandler progressHandler = new ProgressHandler(A.length * B.length, 1);

            if (parallel) {
                Person[] finalB = B;
                Arrays.stream(A).parallel().forEach(a -> {
                    Arrays.stream(finalB).parallel().forEach(b-> {
                        DataHandler.handleDataPoints(a, b, hashAreaSize, hashFunctionCount, threshold, trueMatches, predictedMatches);
                        progressHandler.updateProgress();
                    });
                });
            } else {
                for (Person a : A) {
                    for (Person b : B) {
                        DataHandler.handleDataPoints(a, b, hashAreaSize, hashFunctionCount, threshold, trueMatches, predictedMatches);
                        progressHandler.updateProgress();
                    }
                }
            }
            progressHandler.finish();

            // calculate and print out the stats
            long endTime = System.nanoTime();
            System.out.println("Computation time: " + (endTime - startTime) / 1000000 + " ms");
            PrecisionRecallStats stats = new PrecisionRecallStats(trueMatches, predictedMatches);
            System.out.println(stats);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
