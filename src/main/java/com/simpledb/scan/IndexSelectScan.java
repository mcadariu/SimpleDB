package com.simpledb.scan;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.index.Index;

public class IndexSelectScan implements Scan {
    private TableScan tableScan;
    private Index idx;
    private Constant val;

    public IndexSelectScan(Index idx, Constant val, TableScan ts) throws BufferAbortException, LockAbortException {
        this.tableScan = ts;
        this.idx = idx;
        this.val = val;
        beforeFirst();
    }

    @Override
    public void beforeFirst() throws BufferAbortException, LockAbortException {
        idx.beforeFirst(val);
    }

    @Override
    public boolean next() throws LockAbortException, BufferAbortException {
        boolean ok = idx.next();
        if (ok) {
            RID rid = idx.getDataRid();
            tableScan.moveToRid(rid);
        }
        return ok;
    }

    @Override
    public int getInt(String fldname) throws LockAbortException {
        return tableScan.getInt(fldname);
    }

    @Override
    public String getString(String fldname) throws LockAbortException {
        return tableScan.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) throws LockAbortException {
        return tableScan.getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return tableScan.hasField(fldname);
    }

    @Override
    public void close() {
        idx.close();
        tableScan.close();
    }
}
