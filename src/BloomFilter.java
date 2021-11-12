import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Class for hashing and storing string values into a bloom filter.
 * Uses double hashing and bigrams.
 */
public class BloomFilter {

    boolean[] hashArea;
    int k; // # of hash functions to be simulated

    /**
     * Constructor for BloomFilter instance. Hash area is initialized with all 0's.
     * @param hashAreaSize The length of the hash area.
     * @param hashFunctionCount The number of hash functions to be simulated through double hashing.
     */
    public BloomFilter(int hashAreaSize, int hashFunctionCount) {
        hashArea = new boolean[hashAreaSize];
        Arrays.fill(hashArea, false);
        k = hashFunctionCount;
    }

    /**
     * Splits given string into bigrams and stores all hash values of each bigram into the hash area, by simulating k
     * hash functions through double hashing.
     * @param attrValue attribute value as string.
     * @throws NoSuchAlgorithmException
     */
    public void store(String attrValue) throws NoSuchAlgorithmException {
        // TODO standardize string: remove non-alphanumerics, make all uppercase
        List<String> bigrams = getBigrams(attrValue);
        for (String bigram : bigrams) {
            storeBigram(bigram);
        }
    }

    /**
     * Computes the Jaccard-Similarity (|intersection| / |union|) between this and another given Bloom filter.
     * They both must have the same hash area size.
     * @param other The Bloom Filter to compare this to.
     * @return the similarity coefficient
     */
    public double computeJaccardSimilarity(BloomFilter other) {
        if (other.getHashArea().length != hashArea.length) {
            throw new IllegalArgumentException("Bloom filters must have same hash area size.");
        }
        int union = 0;
        int intersect = 0;
        for (int i = 0; i < hashArea.length; i++) {
            if (hashArea[i] && other.getHashArea()[i]) {
                intersect++;
            } if (hashArea[i] || other.getHashArea()[i]) {
                union++;
            }
        }
        return 1.0 * intersect / union;
    }

    /**
     * Computes the Dice-Similarity (2 * |intersection| / (|X|+|Y|)) between this and another given Bloom filter.
     * They both must have the same hash area size.
     * @param other The Bloom Filter to compare this to.
     * @return the similarity coefficient
     */
    public double computeDiceSimilarity(BloomFilter other) {
        if (other.getHashArea().length != hashArea.length) {
            throw new IllegalArgumentException("Bloom filters must have same hash area size.");
        }
        int unionWithDuplicates = 0; // |X| + |Y|
        int intersect = 0;
        for (int i = 0; i < hashArea.length; i++) {
            if (hashArea[i] && other.getHashArea()[i]) {
                intersect++;
                unionWithDuplicates++;
            } if (hashArea[i] || other.getHashArea()[i]) {
                unionWithDuplicates++;
            }
        }
        return 2.0 * intersect / unionWithDuplicates;
    }

    public boolean[] getHashArea() {
        return hashArea;
    }

    public int getK() {
        return k;
    }

    /**
     * Generates from a given string the bigrams and returns them.
     * @param attrValue attribute value as string.
     * @return ArrayList of bigrams as strings.
     */
    private List<String> getBigrams(String attrValue) {
        List<String> bigrams = new ArrayList<>();
        String paddedAttrValue = "_" + attrValue + "_";
        for (int i = 0; i < paddedAttrValue.length() - 1; i++) {
            bigrams.add(paddedAttrValue.substring(i, i + 2));
        }
        return bigrams;
    }

    /**
     * Simulates k hash functions using double hashing based on SHA-1 and MD5. Then stores given bigram in bloom filter
     * using the simulated hash functions.
     * @param bigram bigram to be stored
     * @throws NoSuchAlgorithmException
     */
    private void storeBigram(String bigram) throws NoSuchAlgorithmException {
        for (int i = 0; i < k; i++){
            int hashValue = getHash(bigram, "SHA-1").multiply(BigInteger.valueOf(i))
                    .add(getHash(bigram, "MD5"))
                    .mod(BigInteger.valueOf(hashArea.length)).intValue();
            hashArea[hashValue] = true;
        }
    }

    /**
     * Function for generating a hash value from a given key using a specified hash-algorithm.
     * @param key the string to be hashed.
     * @param algorithm a string representing the hash algorithm to be used, eg. "MD5".
     * @return resulting hash value as BigInteger.
     * @throws NoSuchAlgorithmException If the specified hash-algorithm is unknown.
     */
    private BigInteger getHash(String key, String algorithm) throws NoSuchAlgorithmException {
        // TODO use keyed hashing?
        MessageDigest digest = MessageDigest.getInstance(algorithm);
        digest.reset();
        digest.update(key.getBytes(StandardCharsets.UTF_8));
        return new BigInteger(1, digest.digest());
    }
}
