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

    public static String getSoundexBlockingKey(Person person) {
        Soundex soundex = new Soundex();
        return soundex.soundex(person.getAttributeValue("firstName"))
                .concat(soundex.soundex(person.getAttributeValue("lastName")))
                .concat(person.getAttributeValue("yearOfBirth"));
    }
}
