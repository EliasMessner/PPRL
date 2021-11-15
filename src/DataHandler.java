import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;

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
            String[] attributes = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); // this regex means split by comma but only if there are an even number of quotation marks ahead
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
    public static Person[][] splitDataBySource(Person[] dataSet) {
        Person[][] resultData = new Person[2][dataSet.length/2];
        int a = 0, b = 0;
        for (Person p : dataSet) {
            if (p.getAttributeValue("sourceID").equals("A")) {
                resultData[0][a] = p;
                a++;
            } else if (p.getAttributeValue("sourceID").equals("B")) {
                resultData[1][b] = p;
                b++;
            }
        }
        assert(a == b);
        return resultData;
    }

    /**
     * Given two data points, create a bloom filter for each one and predict whether they match according to the given
     * threshold.
     * @param a data point a.
     * @param b data point b.
     * @param hashAreaSize size of the hash area of the bloom filter.
     * @param hashFunctionCount the number of hash functions to be simulated using double-hashing.
     * @param threshold the threshold to decide by.
     * @param precisionRecallStats instance to make evaluation on.
     */
    public static void handleDataPoints(Person a, Person b, int hashAreaSize, int hashFunctionCount, double threshold,
                                         PrecisionRecallStats precisionRecallStats) {
        Person[] key = new Person[]{a, b};
        BloomFilter bfa = new BloomFilter(hashAreaSize, hashFunctionCount);
        BloomFilter bfb = new BloomFilter(hashAreaSize, hashFunctionCount);
        try {
            bfa.store(a.concatenateNonIdentifyingAttributes());
            bfb.store(b.concatenateNonIdentifyingAttributes());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        precisionRecallStats.evaluate(Objects.equals(a, b), bfa.computeJaccardSimilarity(bfb) >= threshold);
    }
}
