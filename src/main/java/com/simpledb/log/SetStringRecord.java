package com.simpledb.log;

import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.file.Page;
import com.simpledb.transaction.Transaction;

public class SetStringRecord implements LogRecord {
    private int txnum, offset;
    private String val;
    private BlockId blockId;

    public SetStringRecord(Page p) {
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
        val = p.getString(vpos);
    }

    @Override
    public int op() {
        return SETSTRING;
    }

    @Override
    public int txNumber() {
        return txnum;
    }

    @Override
    public void undo(Transaction tx) {
        tx.pin(blockId);
        tx.setString(blockId, offset, val, false);
        tx.unpin(blockId);
    }

    public static int writeToLog(LogMgr lm, int txnum, BlockId blockId, int offset, String val, FileMgr fileMgr) {

        int tpos = Integer.BYTES;
        int fpos = tpos + Integer.BYTES;
        int bpos = fpos + Page.maxLength(blockId.fileName().length());
        int opos = bpos + Integer.BYTES;
        int vpos = opos + Integer.BYTES;
        int reclen = vpos + Page.maxLength(val.length());
        Page p = new Page(reclen, fileMgr.arena());

        p.setInt(0, SETSTRING);
        p.setInt(tpos, txnum);
        p.setString(fpos, blockId.fileName());
        p.setInt(bpos, blockId.number());
        p.setInt(opos, offset);
        p.setString(vpos, val);
        return lm.append(p.toByteArray());
    }
}
