package dkvs.server.util;

import java.util.Scanner;

public class Entry {
    public final int term;
    public final Operation operation;
    public final String key;
    public final String value;

    public Entry(int term, Operation operation, String key, String value) {
        this.term = term;
        this.operation = operation;
        this.key = key;
        this.value = value;
    }

    public static Entry parseEntry(Scanner sc) {
        return new Entry(sc.nextInt(), Operation.valueOf(sc.next()), sc.next(), sc.nextLine().substring(1));
    }

    public String prettyPrint() {
        return "Entry { term=" + term + ", operation=" + operation + ", key=" + (key == null ? "?" : key) +
                ", value=" + (value == null ? "?" : value) + " }";
    }

    @Override
    public String toString() {
        return term + " " + operation + " " + (key == null ? "?" : key) + " " + (value == null ? "?" : value);
    }
}
