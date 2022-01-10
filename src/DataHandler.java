import org.apache.commons.codec.language.Soundex;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Class responsible for parsing the data from csv file and splitting it by its sourceID.
 */
public class DataHandler {

    /**
     * Reads file and returns array of Person objects.
     * @param filePath the filepath to the cvs file.
     * @param size the number of entries in the dataset.
     * @return array of Person instances represented by the file.
     * @throws IOException
     */
    public static Person[] parseData(String filePath, int size) throws IOException {
        Person[] dataSet = new Person[size];
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line;
        int i = 0;
        while ((line = br.readLine()) != null) {
            if (line.trim().isEmpty()) continue;
            String[] attributes = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); // this regex means split by comma but only if there is an even number of quotation marks ahead
            dataSet[i] = new Person(attributes);
            i++;
        }
        return dataSet;
    }

    /**
     * Splits the dataset into two equally sized subsets by the sourceID attribute. Therefore the dataset is expected to
     * have half the entries with sourceID "A", the other half with sourceID "B".
     * @param dataSet the array to be split.
     * @return a 2D Person-array, the first dimension only containing two entries, each a subset of the dataset.
     */
    public static List<Person[]> splitDataBySource(Person[] dataSet) {
        List<Person> a = new ArrayList<>();
        List<Person> b = new ArrayList<>();
        for (Person p : dataSet) {
            if (p.getAttributeValue("sourceID").equals("A")) {
                a.add(p);
            } else if (p.getAttributeValue("sourceID").equals("B")) {
                b.add(p);
            }
        }
        List<Person[]> resultData = new ArrayList<>();
        resultData.add(a.toArray(Person[]::new));
        resultData.add(b.toArray(Person[]::new));
        return resultData;
    }

    /**
     * Given two Persons and a personBloomFilterMap, compare the corresponding Bloom Filters using Jaccard similarity
     * and decide by a threshold if they match. Then evaluate the true match (globalID matched) and the predicted match
     * using the given precisionRecallStats
     * @param a data point a.
     * @param b data point b.
     * @param personBloomFilterMap Map containing the data points a and b as keys and readily created BloomFilters as values.
     * @param threshold the threshold to decide by.
     * @param precisionRecallStats instance to make evaluation on.
     */
    public static void evaluatePersonPair(Person a, Person b, Map<Person, BloomFilter> personBloomFilterMap, double threshold,
                                          PrecisionRecallStats precisionRecallStats) {
        BloomFilter bfa = personBloomFilterMap.get(a);
        BloomFilter bfb = personBloomFilterMap.get(b);
        precisionRecallStats.evaluate(a.equalGlobalID(b), bfa.computeJaccardSimilarity(bfb) >= threshold);
    }

    public static void createAndStoreBloomFilter(int hashAreaSize, int hashFunctionCount, Person person, Map<Person, BloomFilter>
            personBloomFilterMap, HashingMode mode, boolean weightedAttributes, String paddingString) {
        BloomFilter bf = new BloomFilter(hashAreaSize, hashFunctionCount, mode, paddingString);
        try {
            bf.storePersonData(person, weightedAttributes);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        personBloomFilterMap.put(person, bf);
    }

    public static String getSoundexBlockingKey(Person person) {
        Soundex soundex = new Soundex();
        return soundex.soundex(person.getAttributeValue("firstName"))
                .concat(soundex.soundex(person.getAttributeValue("lastName")))
                .concat(person.getAttributeValue("yearOfBirth"));
    }
}
