package com.simpledb.log;

import com.simpledb.file.Page;

public class StartRecord implements LogRecord {
    private int txnum;

    public StartRecord(Page p) {
        int tpos = Integer.BYTES;
        txnum = p.getInt(tpos);
    }

    @Override
    public int op() {
        return START;
    }

    @Override
    public int txNumber() {
        return txnum;
    }

    @Override
    public void undo(Transaction tx) {
        // nothing to undo
    }

    public static int writeToLog(LogMgr lm, int txnum) {
        byte[] rec = new byte[2 * Integer.BYTES];
        Page p = new Page(rec);
        p.setInt(0, START);
        p.setInt(Integer.BYTES, txnum);
        return lm.append(rec);
    }
}
