package com.simpledb.buffer;

import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.file.Page;
import com.simpledb.log.LogMgr;

public class Buffer {
    private FileMgr fileMgr;
    private LogMgr logMgr;
    private Page contents;
    private BlockId blockId;

    private int pins = 0;
    private int txnum = -1;
    private int lsn = -1;

    public Buffer(FileMgr fileMgr, LogMgr logMgr) {
        this.fileMgr = fileMgr;
        this.logMgr = logMgr;
        contents = new Page(fileMgr.blockSize(), fileMgr.arena());
    }

    public Page contents() {
        return contents;
    }

    public BlockId block() {
        return blockId;
    }

    public void setModified(int txnum, int lsn) {
        this.txnum = txnum;
        if (lsn >= 0) this.lsn = lsn;
    }

    public boolean isPinned() {
        return pins > 0;
    }

    public int modifyingTx() {
        return txnum;
    }

    void assignToBlock(BlockId b) {
        flush();
        blockId = b;
        fileMgr.read(blockId, contents);
        pins = 0;
    }

    void flush() {
        if (txnum >= 0) {
            logMgr.flush(lsn);
            fileMgr.write(blockId, contents);
            txnum = -1;
        }
    }

    void pin() {
        pins++;
    }

    void unpin() {
        pins--;
    }


}
