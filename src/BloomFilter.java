import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Class for hashing and storing string values into a bloom filter.
 * Uses double hashing, enhanced double hashing, triple hashing or random hashing, depending on mode.
 * Uses bigrams.
 */
public class BloomFilter {

    boolean[] hashArea;
    int k; // # of hash functions to be simulated
    HashingMode mode;

    /**
     * Constructor for BloomFilter instance. Hash area is initialized with all 0's.
     * @param hashAreaSize The length of the hash area.
     * @param hashFunctionCount The number of hash functions to be simulated through double hashing.
     */
    public BloomFilter(int hashAreaSize, int hashFunctionCount, HashingMode mode) {
        hashArea = new boolean[hashAreaSize];
        Arrays.fill(hashArea, false);
        k = hashFunctionCount;
        this.mode = mode;
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
     * Simulates k hash functions using specified hashing mode based on SHA-1, MD5 and MD2. Then stores given bigram in
     * bloom filter using the simulated hash functions.
     * @param bigram bigram to be stored
     * @throws NoSuchAlgorithmException
     */
    private void storeBigram(String bigram) throws NoSuchAlgorithmException {
        switch (mode) {
            case DOUBLE_HASHING -> storeBigramDouble(bigram);
            case ENHANCED_DOUBLE_HASHING -> storeBigramEnhancedDouble(bigram);
            case TRIPLE_HASHING -> storeBigramTriple(bigram);
            case RANDOM_HASHING -> storeBigramRandom(bigram);
        }
    }

    private void storeBigramRandom(String bigram) {
        long seed = bigram.charAt(0) + 257 * bigram.charAt(1);
        Random generator = new Random(seed);
        int hits = 0;
        while (hits < k) {
            int hashValue = (int) (generator.nextDouble() * hashArea.length);
            if (hashArea[hashValue]) continue;  // collision
            hashArea[hashValue] = true;
            hits++;
        }
    }

    /**
     * h_i(x) = (h1(x) + i * h2(x) + i^2 * h3(x)) mod m
     */
    private void storeBigramTriple(String bigram) throws NoSuchAlgorithmException {
        BigInteger h1 = getHash(bigram, "MD5");
        BigInteger h2 = getHash(bigram, "SHA-1");
        BigInteger h3 = getHash(bigram, "MD2");
        int i = 0, hits = 0;
        while (hits < k) {
            int o = Math.max(2*i - 1, 0); // i-th odd integer: 0, 1, 3, 5, 7, 9, ...
            int hashValue = h1.mod(BigInteger.valueOf(hashArea.length)).intValue();
            h1 = h1.add(h2)
                    .add(h3.multiply(BigInteger.valueOf(o)));
            i++;
            if (hashArea[hashValue]) continue;  // collision
            hashArea[hashValue] = true;
            hits++;
        }
    }

    private void storeBigramEnhancedDouble(String bigram) throws NoSuchAlgorithmException {
        BigInteger h1 = getHash(bigram, "MD5");
        BigInteger h2 = getHash(bigram, "SHA-1");
        int i = 0, hits = 0;
        while (hits < k) {
            int hashValue = h1.mod(BigInteger.valueOf(hashArea.length)).intValue();
            h1 = h1.add(h2);
            h2 = h2.add(BigInteger.valueOf(i));
            i++;
            if (hashArea[hashValue]) continue;  // collision
            hashArea[hashValue] = true;
            hits++;
        }
    }

    /**
     * h_i(x) = (h1(x) + i * h2(x)) mod m
     */
    private void storeBigramDouble(String bigram) throws NoSuchAlgorithmException {
        BigInteger h1 = getHash(bigram, "MD5");
        BigInteger h2 = getHash(bigram, "SHA-1");
        int hits =0;
        while (hits < k) {
            int hashValue = h1.mod(BigInteger.valueOf(hashArea.length)).intValue();
            h1 = h1.add(h2);
            if (hashArea[hashValue]) continue;  // collision
            hashArea[hashValue] = true;
            hits++;
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
