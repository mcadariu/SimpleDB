package com.simpledb.log;

import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.file.Page;

import java.util.Iterator;
import java.util.function.Consumer;

public class LogIterator implements Iterator<byte[]> {
    private FileMgr fileMgr;
    private BlockId blockId;
    private Page p;
    private int currentpos;
    private int boundary;

    public LogIterator(FileMgr fileMgr, BlockId blockId) {
        this.fileMgr = fileMgr;
        this.blockId = blockId;

        byte[] b = new byte[fileMgr.blockSize()];
        p = new Page(b, fileMgr.arena());
        moveToBlock(blockId);
    }

    private void moveToBlock(BlockId blockId) {
        fileMgr.read(blockId, p);
        boundary = p.getInt(0);
        currentpos = boundary;
    }

    @Override
    public boolean hasNext() {
        return currentpos < fileMgr.blockSize() || blockId.number() > 0;
    }

    @Override
    public byte[] next() {
        if(currentpos >= fileMgr.blockSize()) {
            blockId = new BlockId(blockId.fileName(), blockId.number() - 1);
            moveToBlock(blockId);
        }

        byte[] rec = p.getBytes(currentpos);
        currentpos += Integer.BYTES + rec.length;
        return rec;
    }

    @Override
    public void remove() {
        Iterator.super.remove();
    }

    @Override
    public void forEachRemaining(Consumer<? super byte[]> action) {
        Iterator.super.forEachRemaining(action);
    }
}
