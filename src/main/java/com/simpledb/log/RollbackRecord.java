package com.simpledb.log;

import com.simpledb.file.FileMgr;
import com.simpledb.file.Page;
import com.simpledb.transaction.Transaction;

import java.io.File;

public class RollbackRecord implements LogRecord {
    private int txnum;

    public RollbackRecord(Page p) {
        int tpos = Integer.BYTES;
        txnum = p.getInt(tpos);
    }

    @Override
    public int op() {
        return ROLLBACK;
    }

    @Override
    public int txNumber() {
        return txnum;
    }

    @Override
    public void undo(Transaction tx) {
        // nothing to undo
    }

    public static int writeToLog(LogMgr lm, int txnum, FileMgr fileMgr) {
        Page p = new Page(2 * Integer.BYTES, fileMgr.arena());
        p.setInt(0, ROLLBACK);
        p.setInt(Integer.BYTES, txnum);
        return lm.append(p.toByteArray());
    }
}
