package com.simpledb.scan;

public interface Constant {
    Object asJavaVal();

    boolean equals(Object obj);

    int hashCode();

    String toString();

    int compareTo(Constant other);
}
