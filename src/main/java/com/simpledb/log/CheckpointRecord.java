package com.simpledb.log;

import com.simpledb.file.FileMgr;
import com.simpledb.file.Page;
import com.simpledb.transaction.Transaction;

public class CheckpointRecord implements LogRecord {

    public CheckpointRecord(Page p) {
        // no data to read from the log
    }

    public CheckpointRecord() {
    }

    @Override
    public int op() {
        return CHECKPOINT;
    }

    @Override
    public int txNumber() {
        return -1; // checkpoint records are not associated with a transaction
    }

    @Override
    public void undo(Transaction tx) {
        // nothing to undo
    }

    public static int writeToLog(LogMgr lm, FileMgr fileMgr) {
        Page p = new Page(Integer.BYTES, fileMgr.arena());
        p.setInt(0, CHECKPOINT);
        return lm.append(p.toByteArray());
    }
}
