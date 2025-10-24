package com.simpledb.file;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.scan.TableScan;
import com.simpledb.scan.UpdateScan;
import com.simpledb.transaction.Transaction;

public class TempTable {
    private static int nextTableNum = 0;
    private Transaction tx;
    private String tblname;
    private Layout layout;

    public TempTable(Transaction tx, Schema sch) {
        this.tx = tx;
        tblname = nextTableNum();
        layout = new Layout(sch);
    }

    public UpdateScan open() throws BufferAbortException, LockAbortException {
        return new TableScan(tx, tblname, layout);
    }

    public String tableName() {
        return tblname;
    }

    public Layout getLayout() {
        return layout;
    }

    private synchronized String nextTableNum() {
        nextTableNum++;
        return "temp" + nextTableNum;
    }
}
