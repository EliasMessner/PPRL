package com.company;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DataHandler {

    public static Person[] parseData(String filePath, int size) throws IOException {
        Person[] dataSet = new Person[size];
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line;
        int i = 0;
        while ((line = br.readLine()) != null) {
            if (line.trim().isBlank()) continue;
            String[] attributes = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); // this regex means split by comma but only if there are an even number of quotation marks ahead
            dataSet[i] = new Person(attributes);
            i++;
        }
        return dataSet;
    }

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
        return resultData;
    }
}
