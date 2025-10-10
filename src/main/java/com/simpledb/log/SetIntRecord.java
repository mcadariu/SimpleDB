package com.simpledb.log;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.file.Page;
import com.simpledb.transaction.Transaction;

public class SetIntRecord implements LogRecord {
    private int txnum, offset, val;
    private BlockId blockId;

    public SetIntRecord(Page p) {
        int tpos = Integer.BYTES;
        txnum = p.getInt(tpos);

        int fpos = tpos + Integer.BYTES;
        String filename = p.getString(fpos);

        int bpos = fpos + Page.maxLength(filename.length());
        int blknum = p.getInt(bpos);

        blockId = new BlockId(filename, blknum);
        int opos = bpos + Integer.BYTES;
        offset = p.getInt(opos);
        int vpos = opos + Integer.BYTES;
        val = p.getInt(vpos);
    }

    @Override
    public int op() {
        return SETINT;
    }

    @Override
    public int txNumber() {
        return txnum;
    }

    @Override
    public void undo(Transaction tx) throws BufferAbortException, LockAbortException {
        tx.pin(blockId);
        tx.setInt(blockId, offset, val, false);
        tx.unpin(blockId);
    }

    public static int writeToLog(LogMgr lm, int txnum, BlockId blockId, int offset, int val, FileMgr fileMgr) {
        int tpos = Integer.BYTES;
        int fpos = tpos + Integer.BYTES;
        int bpos = fpos + Page.maxLength(blockId.fileName().length());
        int opos = bpos + Integer.BYTES;
        int vpos = opos + Integer.BYTES;
        int reclen = vpos + Integer.BYTES;
        Page p = new Page(reclen, fileMgr.arena());

        p.setInt(0, SETINT);
        p.setInt(tpos, txnum);
        p.setString(fpos, blockId.fileName());
        p.setInt(bpos, blockId.number());
        p.setInt(opos, offset);
        p.setInt(vpos, val);
        return lm.append(p.toByteArray());
    }
}
