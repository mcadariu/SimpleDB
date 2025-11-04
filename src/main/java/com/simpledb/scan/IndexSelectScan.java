package com.simpledb.scan;

import com.simpledb.index.Index;

public class IndexSelectScan implements Scan {
    private TableScan tableScan;
    private Index idx;
    private Constant val;

    public IndexSelectScan(Index idx, Constant val, TableScan ts) {
        this.tableScan = ts;
        this.idx = idx;
        this.val = val;
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        idx.beforeFirst(val);
    }

    @Override
    public boolean next() {
        boolean ok = idx.next();
        if (ok) {
            RID rid = idx.getDataRid();
            tableScan.moveToRid(rid);
        }
        return ok;
    }

    @Override
    public int getInt(String fldname) {
        return tableScan.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        return tableScan.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) {
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
