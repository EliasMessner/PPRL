import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

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
     * Calculates a linking according to the linking mode set in the parameters record.
     * @return A set of pairs representing the predicted matches.
     */
    public Set<PersonPair> getLinking() {
        return switch (parameters.linkingMode()) {
            case POLYGAMOUS -> getPolygamousLinking();
            case SEMI_MONOGAMOUS_LEFT -> getSemiMonogamousLinking(true);
            case SEMI_MONOGAMOUS_RIGHT -> getSemiMonogamousLinking(false);
            case STABLE_MARRIAGE -> getStableMarriageLinking();
        };
    }

    /**
     * Undirected Linking.
     * Links the data points of the two sources A and B to each other in a stable marriage linking. That means that there exists
     * no records a and b who would both rather be matched with each other than their current partners.
     * @return a set of person pairs representing the predicted matches.
     */
    public Set<PersonPair> getStableMarriageLinking() {
        prepareProgressHandler();
        System.out.println("Linking data points...");
        Set<PersonPair> allPairs = Collections.synchronizedSet(new HashSet<>());
        blockingMap.keySet().parallelStream().forEach(blockingKey -> {
            Set<PersonPair> pairs = new HashSet<>();
            stableMarriageLinkingHelper(blockingMap.get(blockingKey), pairs);
            allPairs.addAll(pairs);
            progressHandler.updateProgress(blockingMap.get(blockingKey).size() * blockingMap.get(blockingKey).size());
        });
        progressHandler.finish();
        return allPairs;
    }

    private void stableMarriageLinkingHelper(Set<Person> blockingSubSet, Set<PersonPair> pairs) {
        List<Person[]> splitData = splitDataBySource(blockingSubSet.toArray(Person[]::new));
        Person[] A = splitData.get(0);
        Person[] B = splitData.get(1);
        Map<Person, Set<Person>> hasProposedTo = new HashMap<>();
        Person freeA = getAnySingle(pairs, A);
        Person favoriteB = getFavoriteB(B, freeA, hasProposedTo);
        while (freeA != null && favoriteB != null) {
            hasProposedTo.putIfAbsent(freeA, new HashSet<>());
            hasProposedTo.get(freeA).add(favoriteB);
            if (isSingle(favoriteB, pairs)) {
                pairs.add(new PersonPair(freeA, favoriteB));
            } else {
                Person currentA = getPartnerOf(favoriteB, pairs);
                assert currentA != null;
                double currentSimilarity = personBloomFilterMap.get(favoriteB).computeJaccardSimilarity(personBloomFilterMap.get(currentA));
                double newSimilarity = personBloomFilterMap.get(favoriteB).computeJaccardSimilarity(personBloomFilterMap.get(freeA));
                if (newSimilarity >= currentSimilarity) {
                    if (!pairs.remove(new PersonPair(currentA, favoriteB))) throw new IllegalStateException();
                    pairs.add(new PersonPair(freeA, favoriteB));
                }
            }
            freeA = getAnySingle(pairs, A);
            favoriteB = getFavoriteB(B, freeA, hasProposedTo);
        }
    }

    private Person getPartnerOf(Person p, Set<PersonPair> pairs) {
        for (PersonPair pair : pairs) {
            if (pair.getA().equals(p)) return pair.getB();
            if (pair.getB().equals(p)) return pair.getA();
        }
        return null;
    }

    private boolean isSingle(Person person, Set<PersonPair> pairs) {
        return pairs.stream().noneMatch(pair -> pair.contains(person));
    }

    private Person getFavoriteB(Person[] Bs, Person freeA, Map<Person, Set<Person>> hasProposedTo) {
        if (freeA == null) return null;
        final AtomicReference<Person> favoriteB = new AtomicReference<>(null);
        final AtomicReference<Double> similarity = new AtomicReference<>(0.0);
        Arrays.stream(Bs).parallel().forEach(B -> {
            if (hasProposedTo.containsKey(freeA) && hasProposedTo.get(freeA).contains(B)) return;
            double newSimilarity = personBloomFilterMap.get(freeA).computeJaccardSimilarity(personBloomFilterMap.get(B));
            if (favoriteB.get() == null || newSimilarity > similarity.get()) {
                favoriteB.set(B);
                similarity.set(newSimilarity);
            }
        });
        return favoriteB.get();
    }

    private Person getAnySingle(Set<PersonPair> pairs, Person[] people) {
        for (Person p : people) {
            if (pairs.stream().noneMatch(pair -> pair.contains(p))) return p;
        }
        return null;
    }

    /**
     * Undirected linking.
     * Links the data points of the two sources to each other in a semi-monogamous manner. That means that each record
     * from source A gets its ideal match from source B. Hence, each A-record can only take part in up to one relation
     * but each B-record can take part in any number of relations.
     * @return a set of person pairs representing the predicted matches.
     */
    public Set<PersonPair> getSemiMonogamousLinking(boolean leftIsMonogamous) {
        prepareProgressHandler();
        System.out.println("Linking data points...");
        Map<Person, Match> linkingWithSimilarities = Collections.synchronizedMap(new HashMap<>());
        blockingMap.keySet().parallelStream().forEach(blockingKey ->
                oneSidedMarriageLinkingHelper(blockingMap.get(blockingKey), linkingWithSimilarities, leftIsMonogamous));
        Set<PersonPair> linking = new HashSet<>();
        for (Person a : linkingWithSimilarities.keySet()) {
            linking.add(new PersonPair(a, linkingWithSimilarities.get(a).getPerson()));
        }
        progressHandler.finish();
        return linking;
    }

    /**
     * Undirected linking.
     * Links the data points of the two sources to each other in a polygamous manner. That means each data point can take
     * part in any number of relations. But each relation is only contained in the resulting set once.
     * @return a set of person pairs representing the predicted matches.
     */
    public Set<PersonPair> getPolygamousLinking() {
        prepareProgressHandler();
        System.out.println("Linking data points...");
        Set<PersonPair> linking = Collections.synchronizedSet(new HashSet<>());
        blockingMap.keySet().parallelStream().forEach(blockingKey ->
                polygamousLinkingHelper(blockingMap.get(blockingKey), linking));
        progressHandler.finish();
        return linking;
    }

    /**
     * Helper method for getOneSidedMarriageLinking
     */
    private void oneSidedMarriageLinkingHelper(Set<Person> blockingSubSet, Map<Person, Match> linking, boolean leftIsMonogamous) {
        List<Person[]> splitData = splitDataBySource(blockingSubSet.toArray(Person[]::new));
        Person[] A = splitData.get(0);
        Person[] B = splitData.get(1);
        Arrays.stream(leftIsMonogamous ? A : B).parallel().forEach(a -> Arrays.stream(leftIsMonogamous ? B : A).parallel().forEach(b-> {
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
     * Helper method for getPolygamousLinking
     */
    private void polygamousLinkingHelper(Set<Person> blockingSubSet, Set<PersonPair> linking) {
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

    private void prepareProgressHandler() {
        progressHandler.reset();
        long totalSize = 0;
        // determine total size for progressHandler
        for (String key : blockingMap.keySet()) {
            totalSize += (long) blockingMap.get(key).size() * blockingMap.get(key).size();
        }
        progressHandler.setTotalSize(totalSize);
    }
}