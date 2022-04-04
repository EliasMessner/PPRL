import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

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
        // get the parameters
        List<Parameters> parametersList = new ArrayList<>();
        List<Result> results = new ArrayList<>();
        String[] requiredArguments = {"out", "parallel", "blockingCheat"};
        ArgumentHelper.checkAllPresent(args, requiredArguments);
        if (args.length == requiredArguments.length) {
            // create parameters in nested for loop
            parametersList = createParametersInNestedForLoop();
        } else if (args.length == requiredArguments.length + 1) {
            // get from csv file
            parametersList = FileHandler.parseParametersListFromFile(args[0]);
        } else if (args.length > requiredArguments.length + 1) {
            // get from line arguments
            parametersList.add(ArgumentHelper.parseParametersFromArguments(args));
        }
        boolean parallel = ArgumentHelper.parseBoolean(args, "parallel", true);
        boolean createFileOutput = ArgumentHelper.parseBoolean(args, "out", true);
        boolean blockingCheat = ArgumentHelper.parseBoolean(args, "blockingCheat", true);
        int i = 1;
        for(Parameters parameters : parametersList) {
            System.out.printf("Iteration %d/%d\n", i, parametersList.size());
            PrecisionRecallStats stats;
            // check if random token salting should be done. Random token salting usage example: put r_1000 as token salting value, to do 1000 iterations in random token salting
            if (parameters.tokenSalting().matches("r_[0-9]+")) {
                List<PrecisionRecallStats> randomTokenSaltingResults = randomTokenSalting(Integer.parseInt(parameters.tokenSalting().split("_")[1]), parameters, dataSet, parallel, String.format("%d/%d", i, parametersList.size()), blockingCheat);
                randomTokenSaltingResults.forEach(precisionRecallStats -> results.add(new Result(parameters, precisionRecallStats)));
            } else {
                stats = mainLoop(parameters, dataSet, parallel, blockingCheat);
                results.add(new Result(parameters, stats));
            }
            i++;
        }
        if (createFileOutput) FileHandler.writeResults(results, "results", true);
    }

    private static PrecisionRecallStats mainLoop(Parameters parameters, Person[] dataSet, boolean parallel, boolean blockingCheat) {
        System.out.println(parameters);
        long startTime = System.currentTimeMillis();
        // create all the bloom filters
        ProgressHandler progressHandler = new ProgressHandler(dataSet.length, 1);
        Map<Person, BloomFilter> personBloomFilterMap = getPersonBloomFilterMap(parameters, dataSet, progressHandler);
        // create the blockingKeyEncoders to generate the blockingMap
        List<BlockingKeyEncoder> blockingKeyEncoders = new ArrayList<>();
        blockingKeyEncoders.add(person -> person.getSoundex("firstName").concat(person.getAttributeValue("yearOfBirth")));
        blockingKeyEncoders.add(person -> person.getSoundex("lastName").concat(person.getAttributeValue("yearOfBirth")));
        blockingKeyEncoders.add(person -> person.getSoundex("firstName").concat(person.getSoundex("lastName")));
        // If blockingCheat turned on, use globalID as additional blocking key to avoid false negatives due to blocking
        if (blockingCheat) blockingKeyEncoders.add(person -> person.getAttributeValue("globalID"));
        // create blockingMap
        Map<String, Set<Person>> blockingMap = getBlockingMap(parameters.blocking(), dataSet, progressHandler, parallel,
                blockingKeyEncoders.toArray(BlockingKeyEncoder[]::new));
        // get the linking
        Linker linker = new Linker(dataSet, progressHandler, parameters, personBloomFilterMap, blockingMap, "A", "B", parallel);
        Set<PersonPair> linking = linker.getLinking();
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
        LinkingMode[] linkingModes = {LinkingMode.POLYGAMOUS};
        HashingMode[] hashingModes = {HashingMode.DOUBLE_HASHING};
        boolean[] bValues = {true};
        boolean[] waValues = {true};
        String[] tsValues = {"r_100"};
        int[] lValues = {1024};
        int[] kValues = {10};
        // double[] tValues = {0.5, 0.525, 0.55, 0.575, 0.6, 0.625, 0.65, 0.675, 0.7, 0.725, 0.75, 0.775, 0.8};
        double[] tValues = {0.75};
        String[] h1Values = {"SHA-1", "SHA-224", "SHA-256"};
        String[] h2Values = {"SHA-384", "MD5", "MD2"};
        for (LinkingMode linkingMode : linkingModes) {
            for (HashingMode hashingMode : hashingModes) {
                for (boolean b : bValues) {
                    for (boolean wa : waValues) {
                        for (String ts : tsValues) {
                            for (int l : lValues) {
                                for (int k : kValues) {
                                    for (double t : tValues) {
                                        for (String h1 : h1Values) {
                                            for (String h2 : h2Values) {
                                                parametersList.add(new Parameters(linkingMode, hashingMode, h1, h2, b, wa, ts, l, k, t));
                                            }
                                        }
                                    }
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
     * Do many iterations of the mainLoop method, in each iteration the token-salting value will be set to a new random
     * character. Return a list of all results of the iterations.
     * Random token salting usage example: put r_1000 as token salting value, to do 1000 iterations in random token salting
     * @param iterations # of iterations to be done
     * @param parameters the parameters - the tokenSalting value will be overwritten in each iteration by a random char
     * @param dataSet the data
     * @param mainIterationFlag A String indicating the number of mainLoop iterations already finished, for example
     *                          (1/4) if in total 4 iterations of mainLoop will be done and this is the first one. The
     *                          string will be printed as output on the console.
     * @return list of precisionRecallStats
     */
    private static List<PrecisionRecallStats> randomTokenSalting(int iterations, Parameters parameters, Person[] dataSet, boolean parallel, String mainIterationFlag, boolean blockingCheat) {
        Random random = new Random();
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        List<PrecisionRecallStats> precisionRecallStatsList = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            System.out.printf("RandomTokenSalting - Iteration %d/%d (%s)\n", i+1, iterations, mainIterationFlag);
            char randomToken = alphabet.charAt(random.nextInt(alphabet.length()));
            Parameters parametersModified = new Parameters(parameters.linkingMode(), parameters.hashingMode(), parameters.h1(), parameters.h2(),
                    parameters.blocking(), parameters.weightedAttributes(), Character.toString(randomToken), // set the parameter for the token salting to the random value
                    parameters.l(), parameters.k(), parameters.t());
            precisionRecallStatsList.add(mainLoop(parametersModified, dataSet, parallel, blockingCheat));
        }
        return precisionRecallStatsList;
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
            BloomFilter bf = new BloomFilter(parameters.l(), parameters.k(), parameters.hashingMode(), parameters.tokenSalting(), parameters.h1(), parameters.h2());
            bf.storePersonData(person, parameters.weightedAttributes());
            personBloomFilterMap.put(person, bf);
            progressHandler.updateProgress();
        });
        progressHandler.finish();
        return personBloomFilterMap;
    }

    /**
     * Assigns each entry in given dataset to a blocking key and returns the resulting map. If blocking is turned off, maps
     * all records to the same blocking key "DUMMY_VALUE".
     * @param blocking true if blocking is turned on, else false
     * @param dataSet array of Person objects to be mapped onto keys
     * @param progressHandler to display progress
     * @return a map that maps each blocking key to a set of records encoded by that key.
     */
    private static Map<String, Set<Person>> getBlockingMap(Boolean blocking, Person[] dataSet, ProgressHandler progressHandler, boolean parallel, BlockingKeyEncoder... blockingKeyEncoders) {
        Map<String, Set<Person>> blockingMap;
        if (!blocking) {
            return Map.ofEntries(entry("DUMMY_VALUE", new HashSet<>(Arrays.asList(dataSet))));
        }
        System.out.println("Creating Blocking Keys...");
        progressHandler.reset();
        progressHandler.setTotalSize(dataSet.length);
        blockingMap = mapRecordsToBlockingKeys(dataSet, progressHandler, parallel, blockingKeyEncoders);
        progressHandler.finish();
        return blockingMap;
    }

    /**
     * Helper method for getBlockingMap.
     */
    private static ConcurrentHashMap<String, Set<Person>> mapRecordsToBlockingKeys(Person[] dataSet, ProgressHandler progressHandler, boolean parallel, BlockingKeyEncoder... blockingKeyEncoders) {
        ConcurrentHashMap<String, Set<Person>> blockingMap = new ConcurrentHashMap<>();
        Stream<Person> stream = Arrays.stream(dataSet);
        if (parallel) stream = stream.parallel();
        stream.forEach(person -> {
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
