package com.simpledb.plan;

import com.simpledb.record.Schema;
import com.simpledb.scan.MergeJoinScan;
import com.simpledb.scan.Scan;
import com.simpledb.scan.SortScan;
import com.simpledb.transaction.Transaction;

import java.util.Arrays;
import java.util.List;

public class MergeJoinPlan implements Plan {
    private Plan p1, p2;
    private String fldname1, fldname2;
    private Schema schema = new Schema();

    public MergeJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
        this.fldname1 = fldname1;
        List<String> sortlist1 = Arrays.asList(fldname1);
        this.p1 = new SortPlan(p1, sortlist1, tx);

        this.fldname2 = fldname2;
        List<String> sortlist2 = Arrays.asList(fldname2);
        this.p2 = new SortPlan(p2, sortlist2, tx);

        schema.addAll(p1.schema());
        schema.addAll(p2.schema());
    }

    public Scan open() {
        Scan s1 = p1.open();
        SortScan s2 = (SortScan) p2.open();
        return new MergeJoinScan(s1, s2, fldname1, fldname2);
    }

    public int blocksAccessed() {
        return p1.blocksAccessed() + p2.blocksAccessed();
    }

    public int recordsOutput() {
        int maxvals = Math.max(p1.distinctValues(fldname1), p2.distinctValues(fldname2));
        return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
    }

    public int distinctValues(String fldname) {
        if (p1.schema().hasField(fldname))
            return p1.distinctValues(fldname);
        else
            return p2.distinctValues(fldname);
    }

    public Schema schema() {
        return schema;
    }
}
