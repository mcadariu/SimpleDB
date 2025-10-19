package com.simpledb.planning;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.metadata.MetadataMgr;
import com.simpledb.metadata.StatInfo;
import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.scan.Scan;
import com.simpledb.scan.TableScan;
import com.simpledb.transaction.Transaction;

public class TablePlan implements Plan {

    private Transaction tx;
    private String tblname;
    private Layout layout;
    private StatInfo si;

    public TablePlan(Transaction tx, String tblname, MetadataMgr metadataMgr) throws BufferAbortException, LockAbortException {
        this.tx = tx;
        this.tblname = tblname;
        layout = metadataMgr.getLayout(tblname, tx);
        si = metadataMgr.getStatInfo(tblname, layout, tx);
    }

    @Override
    public Scan open() throws BufferAbortException, LockAbortException {
        return new TableScan(tx, tblname, layout);
    }

    @Override
    public int blocksAccessed() {
        return si.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return si.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return si.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return layout.schema();
    }
}
