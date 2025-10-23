package com.simpledb.plan;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.record.Schema;
import com.simpledb.scan.ProjectScan;
import com.simpledb.scan.Scan;

import java.util.List;

public class ProjectPlan implements Plan {
    private Plan p;
    private Schema schema = new Schema();

    public ProjectPlan(Plan p, List<String> fieldList) {
        this.p = p;
        for (String fldname : fieldList)
            schema.add(fldname, p.schema());
    }

    public Scan open() throws BufferAbortException, LockAbortException {
        Scan s = p.open();
        return new ProjectScan(s, schema.fields());
    }

    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    public int recordsOutput() {
        return p.recordsOutput();
    }

    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    public Schema schema() {
        return schema;
    }
}
