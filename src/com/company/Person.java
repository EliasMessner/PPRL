package com.company;

import java.util.Arrays;
import java.util.Objects;

public class Person {

    public static final String[] attributeNames = {"sourceID", "globalID", "localID", "firstName", "middleName",
            "lastName", "yearOfBirth", "placeOfBirth", "country", "city", "zip", "gender", "ethnic", "race"};
    public String[] attributeValues;

    public Person(String[] attributes) {
        if (attributes.length != 15) {
            throw new IllegalArgumentException("Attribute array must have 15 elements.");
        }
        if (attributes[0].length() != 1 || (!attributes[0].equals("A") && !attributes[0].equals("B"))) {
            throw new IllegalArgumentException("SourceID must be 1 character either 'A' or 'B'");
        }
        if (attributes[12].length() > 1) {
            throw new IllegalArgumentException("Gender if specified must be 1 character either 'F' or 'M'");
        }
        if (attributes[14].length() > 1) {
            throw new IllegalArgumentException("Race if specified must be 1 character");
        }
        this.attributeValues = attributes;
    }

    public String concatenateNonIdentifyingAttributes() {
        return String.join("", Arrays.copyOfRange(attributeValues, 3, 13));
    }

    public String getAttributeValue(String key) {
        int index = 0;
        while (index <= 13) {
            if (attributeNames[index].equals(key)) {
                return attributeValues[index];
            }
            index++;
        }
        throw new IllegalArgumentException("No such attribute '" + key + "'");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return Objects.equals(attributeValues[1], person.attributeValues[1]);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(attributeValues[1]);
    }
}
