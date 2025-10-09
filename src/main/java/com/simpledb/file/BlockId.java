package com.simpledb.file;

public class BlockId {
    private String filename;
    private int blknum;

    public BlockId(String filename, int blknum) {
        this.filename = filename;
        this.blknum = blknum;
    }

    public String fileName() {
        return filename;
    }

    public int number() {
        return blknum;
    }

    public String toString() {
        return "[file " + filename + ", block" + blknum + "]";
    }

    public boolean equals(Object obj) {
        BlockId blockId = (BlockId) obj;
        return filename.equals(blockId.filename) && blknum == blockId.blknum;
    }
}
