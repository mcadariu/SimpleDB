package com.simpledb.record;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.file.BlockId;
import com.simpledb.transaction.Transaction;

import java.sql.Types;

public class RecordPage {
    public static final int EMPTY = 0, USED = 1;
    private Transaction tx;
    private BlockId blk;
    private Layout layout;

    public RecordPage(Transaction tx, BlockId blockId, Layout layout) throws BufferAbortException {
        this.tx = tx;
        this.blk = blockId;
        this.layout = layout;
        tx.pin(blockId);
    }

    public int getInt(int slot, String fldname) throws LockAbortException {
        int fldpos = offset(slot) + layout.offset(fldname);
        return tx.getInt(blk, fldpos);
    }

    public String getString(int slot, String fldname) throws LockAbortException {
        int fldpos = offset(slot) + layout.offset(fldname);
        return tx.getString(blk, fldpos);
    }

    public void setInt(int slot, String fldname, int val) throws LockAbortException {
        int fldpos = offset(slot) + layout.offset(fldname);
        tx.setInt(blk, fldpos, val, true);
    }

    public void setString(int slot, String fldname, String val) throws LockAbortException {
        int fldpos = offset(slot) + layout.offset(fldname);
        tx.setString(blk, fldpos, val, true);
    }

    public void delete(int slot) throws LockAbortException {
        setFlag(slot, EMPTY);
    }

    public void format() throws LockAbortException {
        int slot = 0;
        while (isValidSlot(slot)) {
            tx.setInt(blk, offset(slot), EMPTY, false);
            Schema schema = layout.schema();
            for (String fldname : schema.fields()) {
                int fldpos = offset(slot) + layout.offset(fldname);
                if (schema.type(fldname) == Types.INTEGER)
                    tx.setInt(blk, fldpos, 0, false);
                else
                    tx.setString(blk, fldpos, "", false);
            }
            slot++;
        }
    }

    public int nextAfter(int slot) throws LockAbortException {
        return searchAfter(slot, USED);
    }

    public int insertAfter(int slot) throws LockAbortException {
        int newslot = searchAfter(slot, EMPTY);
        if (newslot >= 0)
            setFlag(newslot, USED);
        return newslot;
    }

    private int searchAfter(int slot, int flag) throws LockAbortException {
        slot++;
        while (isValidSlot(slot)) {
            if (tx.getInt(blk, offset(slot)) == flag)
                return slot;
            slot++;
        }
        return -1;
    }

    public BlockId block() {
        return blk;
    }


    private boolean isValidSlot(int slot) {
        return offset(slot + 1) <= tx.blockSize();
    }

    private void setFlag(int slot, int flag) throws LockAbortException {
        tx.setInt(blk, offset(slot), flag, true);
    }


    private int offset(int slot) {
        return slot * layout.slotsize();
    }
}
