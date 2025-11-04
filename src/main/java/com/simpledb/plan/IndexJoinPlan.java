package com.simpledb.plan;

import com.simpledb.index.Index;
import com.simpledb.metadata.IndexInfo;
import com.simpledb.record.Schema;
import com.simpledb.scan.IndexJoinScan;
import com.simpledb.scan.Scan;
import com.simpledb.scan.TableScan;

public class IndexJoinPlan implements Plan {
    private Plan p1, p2;
    private IndexInfo ii;
    private String joinField;
    private Schema schema = new Schema();

    public IndexJoinPlan(Plan p1, Plan p2, IndexInfo ii, String joinField) {
        this.p1 = p1;
        this.p2 = p2;
        this.ii = ii;
        this.joinField = joinField;
        schema.addAll(p1.schema());
        schema.addAll(p2.schema());
    }

    @Override
    public Scan open() {
        Scan s = p1.open();
        TableScan ts = (TableScan) p2.open();
        Index idx = ii.open();
        return new IndexJoinScan(s, idx, joinField, ts);
    }

    @Override
    public int blocksAccessed() {
        return p1.blocksAccessed() + (p1.recordsOutput() * ii.blocksAccessed()) + recordsOutput();
    }

    @Override
    public int recordsOutput() {
        return p1.recordsOutput() * ii.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        if (p1.schema().hasField(fldname)) {
            return p1.distinctValues(fldname);
        } else {
            return p2.distinctValues(fldname);
        }
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
