import java.io.IOException;
import java.util.*;
import static java.util.Map.entry;

public class Main {

    /**
     * The Main method. Arguments should be threshold (t), hashAreaSize (l), hashFunctionCount (k), mode,
     * weightedAttributes (wa), blocking (b).
     * Or a csv file with the above arguments. Each line in the csv file represents the one set of parameters and there
     * will be one iteration of the program per line.
     * @param args argument specification, for example "t=0.7 l=1000 k=10 mode=ED b=true wa=true"
     */
    public static void main(String[] args) throws IOException {
        // parse the data from the file
        System.out.println("Parsing Data...");
        Person[] dataSet = DataHandler.parseData("datasets/2021_NCVR_Panse_001/dataset_ncvr_dirty.csv", 200000);
        if (args.length == 1) {
            // go by csv file
            List<Result> results = new ArrayList<>();
            List<Parameters> parametersList = FileHandler.parseParametersListFromFile(args[0]);
            int i = 1;
            for(Parameters parameters : parametersList) {
                System.out.printf("Iteration %d/%d\n", i, parametersList.size());
                PrecisionRecallStats stats = mainLoop(parameters, dataSet);
                results.add(new Result(parameters, stats));
                i++;
            }
            FileHandler.writeResults(results, "results/results.csv");
        } else {
            // go by command line arguments
            mainLoop(ArgumentHelper.parseParametersFromArguments(args), dataSet);
        }
    }

    public static PrecisionRecallStats mainLoop(Parameters parameters, Person[] dataSet) {
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
                    parameters.mode(), parameters.weightedAttributes());
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
