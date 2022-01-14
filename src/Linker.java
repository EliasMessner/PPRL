import java.util.*;

/**
 * Class for linking data points from two sources
 */
public class Linker {

    Person[] dataSet;
    ProgressHandler progressHandler;
    Parameters parameters;
    Map<Person, BloomFilter> personBloomFilterMap;
    Map<String, Set<Person>> blockingMap;
    String sourceNameA;
    String sourceNameB;

    /**
     * Constructor for Linker object that can then be used to perform various linking methods on the data.
     * @param dataSet entire dataset
     * @param progressHandler for showing progress in terminal
     * @param parameters program parameters
     * @param personBloomFilterMap map containing person objects as keys and their BloomFilters as values. See Main.getPersonBloomFilterMap().
     * @param blockingMap map containing the blocking keys and sets of records. See Person.getBlockingMap().
     * @param sourceNameA name of source A
     * @param sourceNameB name of source B
     */
    public Linker(Person[] dataSet, ProgressHandler progressHandler, Parameters parameters, Map<Person, BloomFilter> personBloomFilterMap,
                  Map<String, Set<Person>> blockingMap, String sourceNameA, String sourceNameB) {
        this.dataSet = dataSet;
        this.progressHandler = progressHandler;
        this.parameters = parameters;
        this.personBloomFilterMap = personBloomFilterMap;
        this.sourceNameA = sourceNameA;
        this.sourceNameB = sourceNameB;
        this.blockingMap = blockingMap;
    }

    /**
     * Undirected linking.
     * Links the data points of the two sources to each other in a one-sided marriage manner. That means that each record
     * from source A gets its ideal match from source B. Hence each A-record can only take part in up to one relation
     * but each B-record can take part in any number of relations.
     * @return a set of person pairs representing the predicted matches.
     */
    public Set<PersonPair> getOneSidedMarriageLinking() {
        prepareProgressHandler(blockingMap);
        System.out.println("Linking data points...");
        Map<Person, Match> linkingWithSimilarities = Collections.synchronizedMap(new HashMap<>());
        blockingMap.keySet().parallelStream().forEach(blockingKey ->
                oneSidedMarriageLinkingHelper(blockingMap.get(blockingKey), linkingWithSimilarities));
        Set<PersonPair> linking = new HashSet<>();
        for (Person a : linkingWithSimilarities.keySet()) {
            linking.add(new PersonPair(a, linkingWithSimilarities.get(a).getPerson()));
        }
        progressHandler.finish();
        return linking;
    }

    /**
     * Undirected linking.
     * Links the data points of the two sources to each other in an unstable manner. That means each data point can take
     * part in any number of relations. But each relation is only contained in the resulting set once.
     * @return a set of person pairs representing the predicted matches.
     */
    public Set<PersonPair> getUnstableLinking() {
        prepareProgressHandler(blockingMap);
        System.out.println("Linking data points...");
        Set<PersonPair> linking = Collections.synchronizedSet(new HashSet<>());
        blockingMap.keySet().parallelStream().forEach(blockingKey ->
                unstableLinkingHelper(blockingMap.get(blockingKey), linking));
        progressHandler.finish();
        return linking;
    }

    /**
     * Helper method for getOneSidedMarriageLinking
     */
    private void oneSidedMarriageLinkingHelper(Set<Person> blockingSubSet, Map<Person, Match> linking) {
        List<Person[]> splitData = splitDataBySource(blockingSubSet.toArray(Person[]::new));
        Person[] A = splitData.get(0);
        Person[] B = splitData.get(1);
        Arrays.stream(A).parallel().forEach(a -> Arrays.stream(B).parallel().forEach(b-> {
            double similarity = personBloomFilterMap.get(a).computeJaccardSimilarity(personBloomFilterMap.get(b));
            synchronized (linking) {
                if (similarity >= parameters.t() && (!linking.containsKey(a) || similarity >= linking.get(a).getSimilarity())) {
                    linking.put(a, new Match(b, similarity));
                }
            }
            progressHandler.updateProgress();
        }));
    }

    /**
     * Helper method for getUnstableLinking
     */
    private void unstableLinkingHelper(Set<Person> blockingSubSet, Set<PersonPair> linking) {
        List<Person[]> splitData = splitDataBySource(blockingSubSet.toArray(Person[]::new));
        Person[] A = splitData.get(0);
        Person[] B = splitData.get(1);
        Arrays.stream(A).parallel().forEach(a -> Arrays.stream(B).parallel().forEach(b-> {
            double similarity = personBloomFilterMap.get(a).computeJaccardSimilarity(personBloomFilterMap.get(b));
            if (similarity >= parameters.t()) {
                linking.add(new PersonPair(a, b));
            }
            progressHandler.updateProgress();
        }));
    }

    /**
     * Splits the dataset into two equally sized subsets by the sourceID attribute. Therefor the dataset is expected to
     * have half the entries with sourceID "A", the other half with sourceID "B".
     * @param dataSet the array to be split.
     * @return a 2D Person-array, the first dimension only containing two entries, each a subset of the dataset.
     */
    private List<Person[]> splitDataBySource(Person[] dataSet) {
        List<Person> a = new ArrayList<>();
        List<Person> b = new ArrayList<>();
        for (Person p : dataSet) {
            if (p.getAttributeValue("sourceID").equals(sourceNameA)) {
                a.add(p);
            } else if (p.getAttributeValue("sourceID").equals(sourceNameB)) {
                b.add(p);
            }
        }
        List<Person[]> resultData = new ArrayList<>();
        resultData.add(a.toArray(Person[]::new));
        resultData.add(b.toArray(Person[]::new));
        return resultData;
    }

    private void prepareProgressHandler(Map<String, Set<Person>> blockingMap) {
        progressHandler.reset();
        long totalSize = 0;
        // determine total size for progressHandler
        for (String key : blockingMap.keySet()) {
            totalSize += (long) blockingMap.get(key).size() * blockingMap.get(key).size();
        }
        progressHandler.setTotalSize(totalSize);
    }
}
