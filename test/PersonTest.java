import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

import java.util.Objects;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.*;

class PersonTest {

    Person person;

    @BeforeEach
    void setUp() {
        this.setAttributeNamesAndWeights();
        person = new Person("A,CZ67291,b36b8c16c743ed415c6a7f3fc1a3b859,RONALD,EUGENE,LATTIMER,1953,OH,MOORE,PINEHURST,28374,BURNING TREE,M,NL,W".split(" *, *"));
    }

    void setAttributeNamesAndWeights() {
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
    }

    @org.junit.jupiter.api.Test
    void getAttributeValue() {
        assertEquals(person.getAttributeValue("globalID"), "CZ67291");
        assertEquals(person.getAttributeValue("race"), "W");
    }

    @org.junit.jupiter.api.Test
    void testEquals() {
        Person otherEqual = new Person("A,CZ67291,b36b8c16c743ed415c6a7f3fc1a3b859,RONALD,EUGENE,LATTIMER,1953,OH,MOORE,PINEHURST,28374,BURNING TREE,M,NL,W".split(" *, *"));
        Person otherNotEqual = new Person("A,CZ67291,XYZ,RONALD,EUGENE,LATTIMER,1953,OH,MOORE,PINEHURST,28374,BURNING TREE,M,NL,W".split(" *, *"));
        assertEquals(person, otherEqual);
        assertNotEquals(person, otherNotEqual);
    }

    @org.junit.jupiter.api.Test
    void testHashCode() {
        Person otherEqual = new Person("A,CZ67291,b36b8c16c743ed415c6a7f3fc1a3b859,RONALD,EUGENE,LATTIMER,1953,OH,MOORE,PINEHURST,28374,BURNING TREE,M,NL,W".split(" *, *"));
        Person otherNotEqual = new Person("A,CZ67291,XYZ,RONALD,EUGENE,LATTIMER,1953,OH,MOORE,PINEHURST,28374,BURNING TREE,M,NL,W".split(" *, *"));
        assertEquals(person.hashCode(), otherEqual.hashCode());
        assertNotEquals(person.hashCode(), otherNotEqual.hashCode());
    }
}