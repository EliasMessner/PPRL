import java.io.IOException;
import java.util.*;

public class Main {

    /**
     * The Main method. Arguments should be threshold, hashAreaSize, hashFunctionCount, subsetSize, parallelFlag
     * The subsetSize is the size of the subset to be evaluated, e.g. the first 500 entries of each dataset
     * source. If set to -1, the full dataset will be evaluated.
     * parallelFlag should be "p" or "s" to state whether the program should be executed in parallel or serial mode.
     * @param args argument vector containing threshold, hashAreaSize, hashFunctionCount, (optional) subsetSize.
     */
    public static void main(String[] args) throws IOException {
        double t = Double.parseDouble(ArgumentHelper.parseString(args, "t", null));
        int l = Integer.parseInt(ArgumentHelper.parseString(args, "l", null));
        int k = Integer.parseInt(ArgumentHelper.parseString(args, "k", "10"));
        HashingMode hashingMode = switch (ArgumentHelper.parseString(args, "mode", "DH")) {
            case "DH" -> HashingMode.DOUBLE_HASHING;
            case "ED" -> HashingMode.ENHANCED_DOUBLE_HASHING;
            case "TH" -> HashingMode.TRIPLE_HASHING;
            case "RH" -> HashingMode.RANDOM_HASHING;
            default -> throw new IllegalArgumentException("Unexpected Value for Hashing Mode.");
        };
        boolean weightedAttributes = ArgumentHelper.parseBoolean(args, "wa", false) ||
                ArgumentHelper.parseBoolean(args, "weightedAttributes", false);
        boolean blocking = ArgumentHelper.parseBoolean(args, "b", false) ||
                ArgumentHelper.parseBoolean(args, "blocking", false);

        System.out.println("Mode = " + hashingMode);
        System.out.println("Weighted Attributes = " + weightedAttributes);
        System.out.println("Blocking = " + blocking);
        System.out.printf("t=%f, l=%d, k=%d \n", t, l, k);

        long startTime = System.currentTimeMillis();

        // parse the data from the file
        System.out.println("Parsing Data...");
        Person[] dataSet = DataHandler.parseData("datasets/2021_NCVR_Panse_001/dataset_ncvr_dirty.csv", 200000);

        // create all the bloom filters
        ProgressHandler progressHandler = new ProgressHandler(dataSet.length, 1);
        System.out.println("Creating Bloom Filters...");
        Map<Person, BloomFilter> personBloomFilterMap = Collections.synchronizedMap(new HashMap<>());
        Arrays.stream(dataSet).parallel().forEach(person -> {
            DataHandler.createAndStoreBloomFilter(l, k, person, personBloomFilterMap,
                    hashingMode, weightedAttributes);
            progressHandler.updateProgress();
        });
        progressHandler.finish();

        // create blocking keys
        System.out.println("Creating Blocking Keys...");
        progressHandler.reset();
        progressHandler.setTotalSize(dataSet.length);
        Map<String, Set<Person>> blockingMap = mapRecordsToBlockingKeys(dataSet, progressHandler);
        progressHandler.finish();

        // evaluate the cartesian product of the record-set of each blocking key
        PrecisionRecallStats precisionRecallStats = new PrecisionRecallStats();
        progressHandler.reset();
        long totalSize = 0;
        for (String key : blockingMap.keySet()) {
            totalSize += (long) blockingMap.get(key).size() * blockingMap.get(key).size();
        }
        progressHandler.setTotalSize(totalSize);
        System.out.println("Linking data points...");
        blockingMap.keySet().parallelStream().forEach(blockingKey ->
                evaluateCartesianProduct(blockingMap.get(blockingKey), progressHandler, personBloomFilterMap, t,
                precisionRecallStats));
        progressHandler.finish();

        // calculate and print out the stats
        long endTime = System.currentTimeMillis();
        System.out.println("Computation time: " + (endTime - startTime) + " ms");
        System.out.println(precisionRecallStats);
    }

    /**
     * Splits given Person set into 2 subsets of source A and B. Then iterates the cartesian product of the two subsets
     * and evaluates each pair.
     * @param records all records to be split and evaluated
     * @param progressHandler for showing progress in terminal
     * @param personBloomFilterMap the map containing all the bloom filters belonging to the records
     * @param threshold t
     * @param precisionRecallStats for counting TP, TN, FP, FN
     */
    private static void evaluateCartesianProduct(Set<Person> records, ProgressHandler progressHandler,
                                                 Map<Person, BloomFilter> personBloomFilterMap, double threshold,
                                                 PrecisionRecallStats precisionRecallStats) {
        Person[][] splitData = DataHandler.splitDataBySource(records.toArray(Person[]::new));
        Person[] A = splitData[0];
        Person[] B = splitData[1];
        Arrays.stream(A).parallel().forEach(a -> {
            Arrays.stream(B).parallel().forEach(b-> {
                DataHandler.evaluatePersonPair(a, b, personBloomFilterMap, threshold, precisionRecallStats);
                progressHandler.updateProgress();
            });
        });
    }

    /**
     * Maps each entry in given dataset to a blocking key and returns the resulting map.
     * @param dataSet dataset to be mapped
     * @param progressHandler for showing progress in terminal
     */
    private static Map<String, Set<Person>> mapRecordsToBlockingKeys(Person[] dataSet, ProgressHandler progressHandler) {
        Map<String, Set<Person>> blockingMap = Collections.synchronizedMap(new HashMap<>());
        Arrays.stream(dataSet).parallel().forEach(person -> {
            String key = DataHandler.getBlockingKey(person);
            if (blockingMap.containsKey(key)) {
                blockingMap.get(key).add(person);
            } else {
                blockingMap.put(key, Collections.singleton(person));
            }
            progressHandler.updateProgress();
        });
        return blockingMap;
    }
}
