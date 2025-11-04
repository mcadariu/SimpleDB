package com.simpledb.scan;

public record RID(int blockNumber, int slot) {
    @Override
    public String toString() {
        return "[" + blockNumber + ", " + slot + "]";
    }
}
