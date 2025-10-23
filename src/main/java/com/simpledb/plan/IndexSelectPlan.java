package com.simpledb.plan;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.index.Index;
import com.simpledb.metadata.IndexInfo;
import com.simpledb.record.Schema;
import com.simpledb.scan.Constant;
import com.simpledb.scan.IndexSelectScan;
import com.simpledb.scan.Scan;
import com.simpledb.scan.TableScan;

public class IndexSelectPlan {
    private Plan p;
    private IndexInfo ii;
    private Constant val;

    public IndexSelectPlan(Plan p, IndexInfo ii, Constant val) {
        this.p = p;
        this.ii = ii;
        this.val = val;
    }

    public Scan open() throws BufferAbortException, LockAbortException {
        TableScan ts = (TableScan) p.open();
        Index idx = ii.open();
        return new IndexSelectScan(idx, val, ts);
    }

    public int blocksAccessed() {
        return ii.blocksAccessed() + recordsOutput();
    }

    private int recordsOutput() {
        return ii.recordsOutput();
    }

    public int distinctValues(String fldname) {
        return ii.distinctValues(fldname);
    }

    public Schema schema() {
        return p.schema();
    }
}
