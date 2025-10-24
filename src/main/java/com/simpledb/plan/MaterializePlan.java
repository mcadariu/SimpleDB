package com.simpledb.plan;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.file.TempTable;
import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.scan.Scan;
import com.simpledb.scan.UpdateScan;
import com.simpledb.transaction.Transaction;

public class MaterializePlan implements Plan {
    private Plan srcPlan;
    private Transaction tx;

    public MaterializePlan(Transaction tx, Plan srcPlan) {
        this.srcPlan = srcPlan;
        this.tx = tx;
    }

    public Scan open() throws BufferAbortException, LockAbortException {
        Schema schema = srcPlan.schema();
        TempTable tempTable = new TempTable(tx, schema);
        Scan src = srcPlan.open();
        UpdateScan dest = tempTable.open();

        while (src.next()) {
            dest.insert();
            for (String fldname : schema.fields())
                dest.setVal(fldname, src.getVal(fldname));
        }

        src.close();
        dest.beforeFirst();
        return dest;
    }

    public int blocksAccessed() {
        Layout y = new Layout(srcPlan.schema());
        double rpb = (double) (tx.blockSize() / y.slotsize());
        return (int) Math.ceil(srcPlan.recordsOutput() / rpb);
    }

    public int recordsOutput() {
        return srcPlan.recordsOutput();
    }

    public int distinctValues(String fldname) {
        return srcPlan.distinctValues(fldname);
    }

    public Schema schema() {
        return srcPlan.schema();
    }
}
