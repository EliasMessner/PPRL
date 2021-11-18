import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
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
    public static void main(String[] args) throws IOException {
        double threshold = Double.parseDouble(args[0]);
        int hashAreaSize = Integer.parseInt(args[1]);
        int hashFunctionCount = Integer.parseInt(args[2]);
        int subsetSize = Integer.parseInt(args[3]);
        boolean parallel = args[4].equals("p");

        long startTime = System.currentTimeMillis();
        // parse the data from the file
        System.out.println("Parsing Data...");
        Person[] dataSet = DataHandler.parseData("datasets/2021_NCVR_Panse_001/dataset_ncvr_dirty.csv", 200000);
        Person[][] splitData = DataHandler.splitDataBySource(dataSet);
        Person[] A = splitData[0];
        Person[] B = splitData[1];

        // check if only a subset should be evaluated
        if (subsetSize > 0) {
            A = Arrays.copyOfRange(A, 0, subsetSize);
            B = Arrays.copyOfRange(B, 0, subsetSize);
        }

        // create all the bloom filters
        ProgressHandler progressHandler = new ProgressHandler(dataSet.length, 1);
        System.out.println("Creating Bloom Filters...");

        Map<Person, BloomFilter> personBloomFilterMap = Collections.synchronizedMap(new HashMap<>());
        if (parallel) {
            Arrays.stream(dataSet).parallel().forEach(person -> {
                DataHandler.createAndStoreBloomFilter(hashAreaSize, hashFunctionCount, person, personBloomFilterMap);
                progressHandler.updateProgress();
            });
        } else {
            for (Person person : dataSet) {
                DataHandler.createAndStoreBloomFilter(hashAreaSize, hashFunctionCount, person, personBloomFilterMap);
                progressHandler.updateProgress();
            }
        }
        progressHandler.finish();

        // iterate the data set either parallel or serial
        PrecisionRecallStats precisionRecallStats = new PrecisionRecallStats();
        progressHandler.reset();
        progressHandler.setTotalSize((long) A.length * B.length);
        System.out.println("Linking data points...");
        if (parallel) {
            Person[] finalB = B;
            Arrays.stream(A).parallel().forEach(a -> {
                Arrays.stream(finalB).parallel().forEach(b-> {
                    DataHandler.evaluatePersonPair(a, b, personBloomFilterMap, threshold, precisionRecallStats);
                    progressHandler.updateProgress();
                });
            });
        } else {
            for (Person a : A) {
                for (Person b : B) {
                    DataHandler.evaluatePersonPair(a, b, personBloomFilterMap, threshold, precisionRecallStats);
                    progressHandler.updateProgress();
                }
            }
        }
        progressHandler.finish();

        // calculate and print out the stats
        long endTime = System.currentTimeMillis();
        System.out.println("Computation time: " + (endTime - startTime) + " ms");
        System.out.println(precisionRecallStats);

    }
}
