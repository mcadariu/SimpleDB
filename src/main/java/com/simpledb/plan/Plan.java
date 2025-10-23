package com.simpledb.plan;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.record.Schema;
import com.simpledb.scan.Scan;

public interface Plan {
    public Scan open() throws BufferAbortException, LockAbortException;

    public int blocksAccessed();

    public int recordsOutput();

    public int distinctValues(String fldname);

    public Schema schema();
}
