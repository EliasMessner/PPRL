package com.company;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        double threshold = Double.parseDouble(args[0]);
        int hashAreaSize = Integer.parseInt(args[1]);
        int hashFunctionCount = Integer.parseInt(args[2]);
        try {
            Person[] dataSet = DataHandler.parseData("datasets/2021_NCVR_Panse_001/dataset_ncvr_dirty.csv", 200000);
            Person[][] splitData = DataHandler.splitDataBySource(dataSet);
            Person[] A = splitData[0];
            Person[] B = splitData[1];

            // use subset for now until parallelized
            A = Arrays.copyOfRange(A, 0, 500);
            B = Arrays.copyOfRange(B, 0, 500);

            Map<Person[], Boolean> trueMatches = new HashMap<>(); // key should be two Person Objects
            Map<Person[], Boolean> predictedMatches = new HashMap<>();
            for (Person a : A) {
                for (Person b : B) {
                    Person[] key = new Person[]{a, b};
                    trueMatches.put(key, a.equals(b));
                    BloomFilter bfa = new BloomFilter(hashAreaSize, hashFunctionCount);
                    BloomFilter bfb = new BloomFilter(hashAreaSize, hashFunctionCount);
                    bfa.store(a.concatenateNonIdentifyingAttributes());
                    bfb.store(b.concatenateNonIdentifyingAttributes());
                    predictedMatches.put(key, bfa.computeJaccardSimilarity(bfb) >= threshold);
                }
            }
            PrecisionRecallStats stats = new PrecisionRecallStats(trueMatches, predictedMatches);
            System.out.println(stats);
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
}
