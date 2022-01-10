import java.io.IOException;
import java.util.*;
import static java.util.Map.entry;

public class Main {

    /**
     * The Main method. Arguments should be threshold (t), hashAreaSize (l), hashFunctionCount (k), mode,
     * weightedAttributes (wa), blocking (b).
     * Or a csv file with the above arguments. Each line in the csv file represents one set of parameters and there
     * will be one iteration of the program per line.
     * @param args argument specification, for example "t=0.7 l=1000 k=10 mode=ED b=true wa=true"
     */
    public static void main(String[] args) throws IOException {
        // parse the data from the file
        System.out.println("Parsing Data...");
        Person[] dataSet = DataHandler.parseData("datasets/2021_NCVR_Panse_001/dataset_ncvr_dirty.csv", 200000);
        List<Parameters> parametersList = new ArrayList<>();
        List<Result> results = new ArrayList<>();
        if (args.length == 1) {
            // go by csv file
            parametersList = FileHandler.parseParametersListFromFile(args[0]);
        } else if (args.length > 1) {
            // go by command line arguments
            parametersList.add(ArgumentHelper.parseParametersFromArguments(args));
        } else {
            // create parameters in nested for loop
            parametersList = createParametersInNestedForLoop(dataSet);
        }
        int i = 1;
        for(Parameters parameters : parametersList) {
            System.out.printf("Iteration %d/%d\n", i, parametersList.size());
            PrecisionRecallStats stats = mainLoop(parameters, dataSet);
            results.add(new Result(parameters, stats));
            i++;
        }
        FileHandler.writeResults(results, "results", true);
    }

    private static PrecisionRecallStats mainLoop(Parameters parameters, Person[] dataSet) {
        System.out.println(parameters);
        long startTime = System.currentTimeMillis();
        // create all the bloom filters
        ProgressHandler progressHandler = new ProgressHandler(dataSet.length, 1);
        Map<Person, BloomFilter> personBloomFilterMap = getPersonBloomFilterMap(parameters, dataSet, progressHandler);
        progressHandler.finish();
        // create blocking keys if blocking is turned on
        Map<String, Set<Person>> blockingMap = getBlockingMap(parameters, dataSet, progressHandler);
        // create resulting stats for precision and recall etc
        PrecisionRecallStats precisionRecallStats = getPrecisionRecallStats(parameters, dataSet);
        // evaluate the cartesian product of the record-set of each blocking key
        progressHandler.reset();
        iterateBlockingMap(parameters, progressHandler, personBloomFilterMap, blockingMap, precisionRecallStats);
        progressHandler.finish();
        // calculate and print out the stats
        long endTime = System.currentTimeMillis();
        System.out.println("Computation time: " + (endTime - startTime) + " ms");
        System.out.println(precisionRecallStats);
        System.out.println("\n\n");
        return precisionRecallStats;
    }

    private static List<Parameters> createParametersInNestedForLoop(Person[] dataSet) {
        List<Parameters> parametersList = new ArrayList<>();
        HashingMode[] modes = {HashingMode.DOUBLE_HASHING, HashingMode.ENHANCED_DOUBLE_HASHING};
        boolean[] bValues = {true};
        boolean[] waValues = {true, false};
        String[] psValues = {"", "X", "123"};
        int[] lValues = {1024};
        int[] kValues = {10};
        double[] tValues = {0.5, 0.525, 0.55, 0.575, 0.6, 0.625, 0.65, 0.675, 0.7, 0.725, 0.75, 0.775, 0.8};
        for (HashingMode mode : modes) {
            for (boolean b : bValues) {
                for (boolean wa : waValues) {
                    for (String ps : psValues) {
                        for (int l : lValues) {
                            for (int k : kValues) {
                                for (double t : tValues) {
                                    parametersList.add(new Parameters(mode, b, wa, ps, l, k, t));
                                }
                            }
                        }
                    }
                }
            }
        }
        return parametersList;
    }

    private static void iterateBlockingMap(Parameters parameters, ProgressHandler progressHandler,
                                           Map<Person, BloomFilter> personBloomFilterMap,
                                           Map<String, Set<Person>> blockingMap, PrecisionRecallStats precisionRecallStats) {
        long totalSize = 0;
        // determine total size for progressHandler
        for (String key : blockingMap.keySet()) {
            totalSize += (long) blockingMap.get(key).size() * blockingMap.get(key).size();
        }
        progressHandler.setTotalSize(totalSize);
        System.out.println("Linking data points...");
        blockingMap.keySet().parallelStream().forEach(blockingKey ->
                evaluateCartesianProduct(blockingMap.get(blockingKey), progressHandler, personBloomFilterMap, parameters.t(),
                        precisionRecallStats));
    }

    private static PrecisionRecallStats getPrecisionRecallStats(Parameters parameters, Person[] dataSet) {
        PrecisionRecallStats precisionRecallStats;
        if (parameters.blocking()) {
            precisionRecallStats = new PrecisionRecallStatsForBlocking(((long) dataSet.length * dataSet.length)/4, 20000);
        } else {
            precisionRecallStats = new PrecisionRecallStats();
        }
        return precisionRecallStats;
    }

    private static Map<String, Set<Person>> getBlockingMap(Parameters parameters, Person[] dataSet, ProgressHandler progressHandler) {
        Map<String, Set<Person>> blockingMap;
        if (parameters.blocking()) {
            System.out.println("Creating Blocking Keys...");
            progressHandler.reset();
            progressHandler.setTotalSize(dataSet.length);
            blockingMap = mapRecordsToBlockingKeys(dataSet, progressHandler);
            progressHandler.finish();
        } else {
            blockingMap = Map.ofEntries(
                    entry("DUMMY_VALUE", new HashSet<>(Arrays.asList(dataSet)))
            );
        }
        return blockingMap;
    }

    private static Map<Person, BloomFilter> getPersonBloomFilterMap(Parameters parameters, Person[] dataSet,
                                                                    ProgressHandler progressHandler) {
        System.out.println("Creating Bloom Filters...");
        Map<Person, BloomFilter> personBloomFilterMap = Collections.synchronizedMap(new HashMap<>());
        Arrays.stream(dataSet).parallel().forEach(person -> {
            DataHandler.createAndStoreBloomFilter(parameters.l(), parameters.k(), person, personBloomFilterMap,
                    parameters.mode(), parameters.weightedAttributes(), parameters.paddingString());
            progressHandler.updateProgress();
        });
        return personBloomFilterMap;
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
        List<Person[]> splitData = DataHandler.splitDataBySource(records.toArray(Person[]::new));
        Person[] A = splitData.get(0);
        Person[] B = splitData.get(1);
        Arrays.stream(A).parallel().forEach(a -> Arrays.stream(B).parallel().forEach(b-> {
            DataHandler.evaluatePersonPair(a, b, personBloomFilterMap, threshold, precisionRecallStats);
            progressHandler.updateProgress();
        }));
    }

    /**
     * Maps each entry in given dataset to a blocking key and returns the resulting map.
     * @param dataSet dataset to be mapped
     * @param progressHandler for showing progress in terminal
     */
    private static Map<String, Set<Person>> mapRecordsToBlockingKeys(Person[] dataSet, ProgressHandler progressHandler) {
        Map<String, Set<Person>> blockingMap = Collections.synchronizedMap(new HashMap<>());
        Arrays.stream(dataSet).parallel().forEach(person -> {
            String soundexBlockingKey = DataHandler.getSoundexBlockingKey(person);
            blockingMap.putIfAbsent(soundexBlockingKey, new HashSet<>());
            blockingMap.get(soundexBlockingKey).add(person);
            progressHandler.updateProgress();
        });
        return blockingMap;
    }
}
