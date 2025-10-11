package com.simpledb.scan;

public class RID {
    private int blknum;
    private int slot;

    public RID(int blknum, int slot) {
        this.blknum = blknum;
        this.slot = slot;
    }

    public int blockNumber() {
        return blknum;
    }

    public int slot() {
        return slot;
    }

    public boolean equals(Object obj) {
        RID r = (RID) obj;
        return blknum == r.blknum && slot == r.slot;
    }

    public String toString() {
        return "[" + blknum + ", " + slot + "]";
    }
}
