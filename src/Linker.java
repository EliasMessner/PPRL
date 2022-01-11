import java.util.*;

public class Linker {

    Person[] dataSet;
    ProgressHandler progressHandler;
    Parameters parameters;
    Map<Person, BloomFilter> personBloomFilterMap;

    public Linker(Person[] dataSet, ProgressHandler progressHandler, Parameters parameters, Map<Person, BloomFilter> personBloomFilterMap) {
        this.dataSet = dataSet;
        this.progressHandler = progressHandler;
        this.parameters = parameters;
        this.personBloomFilterMap = personBloomFilterMap;
    }

    public Set<PersonPair> getOneSidedMarriageLinking(Map<String, Set<Person>> blockingMap) {
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

    private void oneSidedMarriageLinkingHelper(Set<Person> blockingSubSet, Map<Person, Match> linking) {
        List<Person[]> splitData = DataHandler.splitDataBySource(blockingSubSet.toArray(Person[]::new));
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

    public Set<PersonPair> getUnstableLinking(Map<String, Set<Person>> blockingMap) {
        prepareProgressHandler(blockingMap);
        System.out.println("Linking data points...");
        Set<PersonPair> linking = Collections.synchronizedSet(new HashSet<>());
        blockingMap.keySet().parallelStream().forEach(blockingKey ->
                unstableLinkingHelper(blockingMap.get(blockingKey), linking));
        progressHandler.finish();
        return linking;
    }

    private void unstableLinkingHelper(Set<Person> blockingSubSet, Set<PersonPair> linking) {
        List<Person[]> splitData = DataHandler.splitDataBySource(blockingSubSet.toArray(Person[]::new));
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
