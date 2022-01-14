import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import static java.util.Map.entry;

public class Main {

    public static void main(String[] args) throws IOException {
        Person.setAttributeNamesAndWeights(
                entry("sourceID", 0.0),
                entry("globalID", 0.0),
                entry("localID", 0.0),
                entry("firstName", 2.0),
                entry("middleName", 0.5),
                entry("lastName", 1.5),
                entry("yearOfBirth", 2.5),
                entry("placeOfBirth", 0.5),
                entry("country", .5),
                entry("city", .5),
                entry("zip", .3),
                entry("street", .3),
                entry("gender", 1.0),
                entry("ethnic", 1.0),
                entry("race", 1.0)
        );
        // parse the data from the file
        System.out.println("Parsing Data...");
        Person[] dataSet = FileHandler.parseData("datasets/2021_NCVR_Panse_001/dataset_ncvr_dirty.csv", 200000);
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
        // create blocking keys, use globalID as additional blocking key to avoid false negatives due to blocking
        Map<String, Set<Person>> blockingMap = getBlockingMap(parameters.blocking(), dataSet, progressHandler,
                Person::getSoundexBlockingKey, person -> person.getAttributeValue("globalID"));
        // get the linking
        Linker linker = new Linker(dataSet, progressHandler, parameters, personBloomFilterMap, blockingMap, "A", "B");
        //Set<PersonPair> linking = linker.getOneSidedMarriageLinking();
        Set<PersonPair> linking = linker.getUnstableLinking();
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

    /**
     * Creates a BloomFilter for each Person object in given dataset and returns a map with Person as keys and
     * BloomFilter as values.
     */
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

    /**
     * Maps each entry in given dataset to a blocking key and returns the resulting map. If blocking is turned off, maps
     * all records to the same blocking key "DUMMY_VALUE".
     * @param blocking true if blocking is turned on, else false
     * @param dataSet array of Person objects to be mapped onto keys
     * @param progressHandler to display progress
     * @return a map that maps each blocking key to a set of records encoded by that key.
     */
    private static Map<String, Set<Person>> getBlockingMap(Boolean blocking, Person[] dataSet, ProgressHandler progressHandler, BlockingKeyEncoder... blockingKeyEncoders) {
        Map<String, Set<Person>> blockingMap;
        if (!blocking) {
            return Map.ofEntries(entry("DUMMY_VALUE", new HashSet<>(Arrays.asList(dataSet))));
        }
        System.out.println("Creating Blocking Keys...");
        progressHandler.reset();
        progressHandler.setTotalSize(dataSet.length);
        blockingMap = mapRecordsToBlockingKeys(dataSet, progressHandler, blockingKeyEncoders);
        progressHandler.finish();
        return blockingMap;
    }

    /**
     * Helper method for getBlockingMap.
     */
    private static Map<String, Set<Person>> mapRecordsToBlockingKeys(Person[] dataSet, ProgressHandler progressHandler, BlockingKeyEncoder... blockingKeyEncoders) {
        Map<String, Set<Person>> blockingMap = new ConcurrentHashMap<>();
        Arrays.stream(dataSet).parallel().forEach(person -> {
            for (BlockingKeyEncoder blockingKeyEncoder : blockingKeyEncoders) {
                String blockingKey = blockingKeyEncoder.encode(person);
                blockingMap.putIfAbsent(blockingKey, new HashSet<>());
                blockingMap.get(blockingKey).add(person);
            }
            progressHandler.updateProgress();
        });
        return blockingMap;
    }
}
