package com.simpledb.plan;

import com.simpledb.concurrency.LockAbortException;
import com.simpledb.scan.Constant;
import com.simpledb.scan.Scan;

public interface AggregationFn {
    public String fieldName();
    public void processFirst(Scan s) throws LockAbortException;
    public void processNext(Scan s) throws LockAbortException;
    public Constant value();
}
