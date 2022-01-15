public class BloomFilterMock extends BloomFilter {

    Person person;
    Person[] preferences;

    /**
     * Mock class for testing Linker.getStableMarriageLinking
     * @param person person represented by this Bloom Filter
     * @param preferences list of people preferred by this person, from most favorite (similar) to least favorite (least
     *                    similar)
     */
    public BloomFilterMock(Person person, Person[] preferences) {
        this.person = person;
        this.preferences = preferences;
    }

    /**
     * Returns a higher value the lower the other persons position on this persons preference list is.
     * @param other The Bloom Filter to compare this to.
     * @return mocked distance value
     */
    @Override
    public double computeJaccardSimilarity(BloomFilter other) {
        if (!(other instanceof BloomFilterMock)) return -1.0;
        int distance = 0;
        for (int i = this.preferences.length - 1; i >= 0; i--) {
            if (preferences[i].equals(((BloomFilterMock) other).person)) break;
            distance++;
        }
        return distance;
    }
}
