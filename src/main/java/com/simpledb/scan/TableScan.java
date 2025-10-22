package com.simpledb.scan;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.file.BlockId;
import com.simpledb.record.Layout;
import com.simpledb.record.RecordPage;
import com.simpledb.transaction.Transaction;

import java.sql.Types;

public class TableScan implements UpdateScan {
    private Transaction transaction;
    private Layout layout;
    private RecordPage recordPage;
    private String filename;
    private int currentslot;

    public TableScan(Transaction transaction, String tblname, Layout layout) throws LockAbortException, BufferAbortException {
        this.transaction = transaction;
        this.layout = layout;
        filename = tblname + ".tbl";
        if (transaction.size(filename) == 0) {
            moveToNewBlock();
        } else {
            moveToBlock(0);
        }
    }

    public void close() {
        if (recordPage != null) {
            transaction.unpin(recordPage.block());
        }
    }

    public void beforeFirst() throws BufferAbortException {
        moveToBlock(0);
    }

    public boolean next() throws LockAbortException, BufferAbortException {
        currentslot = recordPage.nextAfter(currentslot);

        while (currentslot < 0) {
            if (atLastBlock())
                return false;
            moveToBlock(recordPage.block().number() + 1);
            currentslot = recordPage.nextAfter(currentslot);
        }
        return true;
    }

    public int getInt(String fldname) throws LockAbortException {
        return recordPage.getInt(currentslot, fldname);
    }

    public String getString(String fldname) throws LockAbortException {
        return recordPage.getString(currentslot, fldname);
    }

    public Constant getVal(String fldname) throws LockAbortException {
        if (layout.schema().type(fldname) == Types.INTEGER)
            return new IntConstant(getInt(fldname));
        else
            return new StringConstant(getString(fldname));
    }

    public boolean hasField(String fldname) {
        return layout.schema().hasField(fldname);
    }

    public void setInt(String fldname, int val) throws LockAbortException {
        recordPage.setInt(currentslot, fldname, val);
    }

    public void setString(String fldname, String val) throws LockAbortException {
        recordPage.setString(currentslot, fldname, val);
    }

    public void setVal(String fldname, Constant val) throws LockAbortException {
        if (layout.schema().type(fldname) == Types.INTEGER)
            setInt(fldname, (Integer) val.asJavaVal());
        else setString(fldname, (String) val.asJavaVal());
    }

    public void insert() throws LockAbortException, BufferAbortException {
        currentslot = recordPage.insertAfter(currentslot);
        while (currentslot < 0) {
            if (atLastBlock())
                moveToNewBlock();
            else
                moveToBlock(recordPage.block().number() + 1);
            currentslot = recordPage.insertAfter(currentslot);
        }
    }

    public void delete() throws LockAbortException {
        recordPage.delete(currentslot);
    }

    public void moveToRid(RID rid) throws BufferAbortException {
        close();
        BlockId blockId = new BlockId(filename, rid.blockNumber());
        recordPage = new RecordPage(transaction, blockId, layout);
        currentslot = rid.slot();
    }

    public RID getRid() {
        return new RID(recordPage.block().number(), currentslot);
    }

    private void moveToBlock(int blknum) throws BufferAbortException {
        close();
        BlockId blockId = new BlockId(filename, blknum);
        recordPage = new RecordPage(transaction, blockId, layout);
        currentslot = -1;
    }

    private void moveToNewBlock() throws LockAbortException, BufferAbortException {
        close();
        BlockId blockId = transaction.append(filename);
        recordPage = new RecordPage(transaction, blockId, layout);
        recordPage.format();
        currentslot = -1;
    }

    private boolean atLastBlock() throws LockAbortException {
        return recordPage.block().number() == transaction.size(filename) - 1;
    }
}
