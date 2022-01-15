import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import static java.util.Map.entry;

class LinkerTest {

    Linker linker;

    @BeforeEach
    void setUp() {
        Person.setAttributeNamesAndWeights(entry("sourceID", 0.0), entry("id", 0.0));
        Person p0 = new Person("A", "0");
        Person p1 = new Person("A", "1");
        Person p2 = new Person("A", "2");
        Person p3 = new Person("A", "3");
        Person p4 = new Person("B", "4");
        Person p5 = new Person("B", "5");
        Person p6 = new Person("B", "6");
        Person p7 = new Person("B", "7");
        Person[] people = new Person[] { p0, p1, p2, p3, p4, p5, p6, p7 };
        Parameters parameters = new Parameters(HashingMode.DOUBLE_HASHING, false, false, "", -1, -1, -1.0); // only dummy values needed
        Map<Person, BloomFilter> personBloomFilterMap = new ConcurrentHashMap<>();

        personBloomFilterMap.put(p0, new BloomFilterMock(p0, new Person[]{p7, p5, p6, p4}));
        personBloomFilterMap.put(p1, new BloomFilterMock(p1, new Person[]{p5, p4, p6, p7}));
        personBloomFilterMap.put(p2, new BloomFilterMock(p2, new Person[]{p4, p5, p6, p7}));
        personBloomFilterMap.put(p3, new BloomFilterMock(p3, new Person[]{p4, p5, p6, p7}));
        personBloomFilterMap.put(p4, new BloomFilterMock(p4, new Person[]{p0, p1, p2, p3}));
        personBloomFilterMap.put(p5, new BloomFilterMock(p5, new Person[]{p0, p1, p2, p3}));
        personBloomFilterMap.put(p6, new BloomFilterMock(p6, new Person[]{p0, p1, p2, p3}));
        personBloomFilterMap.put(p7, new BloomFilterMock(p7, new Person[]{p0, p1, p2, p3}));

        Map<String, Set<Person>> blockingMap = Map.ofEntries(entry("DUMMY_VALUE", new HashSet<>(Arrays.asList(people))));

        linker = new Linker(people, new ProgressHandler(16, 1),
                parameters, personBloomFilterMap, blockingMap, "A", "B");
    }

    @Test
    void getStableMarriageLinking() {
        Person p0 = new Person("A", "0");
        Person p1 = new Person("A", "1");
        Person p2 = new Person("A", "2");
        Person p3 = new Person("A", "3");
        Person p4 = new Person("B", "4");
        Person p5 = new Person("B", "5");
        Person p6 = new Person("B", "6");
        Person p7 = new Person("B", "7");
        Set<PersonPair> linking = linker.getStableMarriageLinking();
        Assertions.assertTrue(linking.contains(new PersonPair(p4, p2)));
        Assertions.assertTrue(linking.contains(new PersonPair(p5, p1)));
        Assertions.assertTrue(linking.contains(new PersonPair(p6, p3)));
        Assertions.assertTrue(linking.contains(new PersonPair(p7, p0)));
    }
}