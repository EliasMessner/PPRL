import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class PersonPair {

    private Person A;
    private Person B;
    private Set<Person> set;

    public PersonPair(Person a, Person b) {
        A = a;
        B = b;
        set = new HashSet<>();
        set.add(A);
        set.add(B);
    }

    public Person getA() {
        return A;
    }

    public Person getB() {
        return B;
    }

    public void set(Person a, Person b) {
        A = a;
        B = b;
        set.clear();
        set.add(A);
        set.add(B);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersonPair that = (PersonPair) o;
        return Objects.equals(set, that.set);
    }

    @Override
    public int hashCode() {
        return Objects.hash(set);
    }
}
