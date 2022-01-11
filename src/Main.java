import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Math.max;
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
            parametersList = createParametersInNestedForLoop();
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
        // create blocking keys
        Map<String, Set<Person>> blockingMap = getBlockingMap(parameters, dataSet, progressHandler);
        // get the linking
        Linker linker = new Linker(dataSet, progressHandler, parameters, personBloomFilterMap);
        //Set<PersonPair> linking = linker.getOneSidedMarriageLinking(blockingMap);
        Set<PersonPair> linking = linker.getUnstableLinking(blockingMap);
        // evaluate
        PrecisionRecallStats precisionRecallStats = new PrecisionRecallStats(100000L * 100000, 20000);
        precisionRecallStats.evaluateAll(linking);
        // output and return
        long endTime = System.currentTimeMillis();
        System.out.println("Computation time: " + (endTime - startTime) + " ms");
        System.out.println(precisionRecallStats);
        System.out.println("\n\n");
        return precisionRecallStats;
    }

    private static List<Parameters> createParametersInNestedForLoop() {
        List<Parameters> parametersList = new ArrayList<>();
        HashingMode[] modes = {HashingMode.DOUBLE_HASHING, HashingMode.ENHANCED_DOUBLE_HASHING};
        boolean[] bValues = {true};
        boolean[] waValues = {true, false};
        String[] tsValues = {"", "X", "123"};
        int[] lValues = {1024};
        int[] kValues = {10};
        double[] tValues = {0.5, 0.525, 0.55, 0.575, 0.6, 0.625, 0.65, 0.675, 0.7, 0.725, 0.75, 0.775, 0.8};
        for (HashingMode mode : modes) {
            for (boolean b : bValues) {
                for (boolean wa : waValues) {
                    for (String ts : tsValues) {
                        for (int l : lValues) {
                            for (int k : kValues) {
                                for (double t : tValues) {
                                    parametersList.add(new Parameters(mode, b, wa, ts, l, k, t));
                                }
                            }
                        }
                    }
                }
            }
        }
        return parametersList;
    }

    private static Map<Person, BloomFilter> getPersonBloomFilterMap(Parameters parameters, Person[] dataSet,
                                                                    ProgressHandler progressHandler) {
        progressHandler.reset();
        System.out.println("Creating Bloom Filters...");
        Map<Person, BloomFilter> personBloomFilterMap = new ConcurrentHashMap<>();
        Arrays.stream(dataSet).parallel().forEach(person -> {
            BloomFilter bf = new BloomFilter(parameters.l(), parameters.k(), parameters.mode(), parameters.tokenSalting());
            bf.storePersonData(person, parameters.weightedAttributes());
            personBloomFilterMap.put(person, bf);
            progressHandler.updateProgress();
        });
        progressHandler.finish();
        return personBloomFilterMap;
    }

    private static Map<String, Set<Person>> getBlockingMap(Parameters parameters, Person[] dataSet, ProgressHandler progressHandler) {
        Map<String, Set<Person>> blockingMap;
        if (!parameters.blocking()) {
            return Map.ofEntries(entry("DUMMY_VALUE", new HashSet<>(Arrays.asList(dataSet))));
        }
        System.out.println("Creating Blocking Keys...");
        progressHandler.reset();
        progressHandler.setTotalSize(dataSet.length);
        blockingMap = mapRecordsToBlockingKeys(dataSet, progressHandler);
        progressHandler.finish();
        return blockingMap;
    }

    /**
     * Maps each entry in given dataset to a blocking key and returns the resulting map.
     * @param dataSet dataset to be mapped
     * @param progressHandler for showing progress in terminal
     */
    private static Map<String, Set<Person>> mapRecordsToBlockingKeys(Person[] dataSet, ProgressHandler progressHandler) {
        Map<String, Set<Person>> blockingMap = new ConcurrentHashMap<>();
        Arrays.stream(dataSet).parallel().forEach(person -> {
            String soundexBlockingKey = DataHandler.getSoundexBlockingKey(person);
            blockingMap.putIfAbsent(soundexBlockingKey, new HashSet<>());
            blockingMap.get(soundexBlockingKey).add(person);
            // add globalID as blocking key in order to avoid false negatives caused by blocking
            String globalID = person.getAttributeValue("globalID");
            blockingMap.putIfAbsent(globalID, new HashSet<>());
            blockingMap.get(globalID).add(person);
            progressHandler.updateProgress();
        });
        return blockingMap;
    }
}
