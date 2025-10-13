package com.simpledb.metadata;

public class StatInfo {
    private int numBlocks;
    private int numRecs;

    public StatInfo(int numBlocks, int numRecs) {
        this.numBlocks = numBlocks;
        this.numRecs = numRecs;
    }

    public int blocksAccessed() {
        return numBlocks;
    }

    public int recordsOutput() {
        return numRecs;
    }

    public int distinctValues(String fldname) {
        return 1 + (numRecs / 3);
    }
}
