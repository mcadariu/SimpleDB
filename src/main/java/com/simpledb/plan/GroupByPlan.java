package com.simpledb.plan;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.record.Schema;
import com.simpledb.scan.GroupByScan;
import com.simpledb.scan.Scan;
import com.simpledb.transaction.Transaction;

import java.util.List;

public class GroupByPlan implements Plan {
    private Plan p;
    private List<String> groupfields;
    private List<AggregationFn> aggregationFns;
    private Schema sch = new Schema();

    public GroupByPlan(Transaction tx, Plan p, List<String> groupfields, List<AggregationFn> aggregationFns) {
        this.p = new SortPlan(p, groupfields, tx);
        this.groupfields = groupfields;
        this.aggregationFns = aggregationFns;

        for (String fldname : groupfields) {
            sch.add(fldname, p.schema());
        }

        for (AggregationFn aggregationFn : aggregationFns) {
            sch.addIntField(aggregationFn.fieldName());
        }
    }

    public Scan open() throws BufferAbortException, LockAbortException {
        Scan s = p.open();
        return new GroupByScan(s, groupfields, aggregationFns);
    }

    @Override
    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        int numgroups = 1;
        for (String fldname : groupfields) {
            numgroups *= p.distinctValues(fldname);
        }
        return numgroups;
    }

    @Override
    public int distinctValues(String fldname) {
        if (p.schema().hasField(fldname))
            return p.distinctValues(fldname);
        else
            return recordsOutput();
    }

    @Override
    public Schema schema() {
        return sch;
    }
}
