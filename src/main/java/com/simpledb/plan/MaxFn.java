package com.simpledb.plan;

import com.simpledb.concurrency.LockAbortException;
import com.simpledb.scan.Constant;
import com.simpledb.scan.Scan;

public class MaxFn implements AggregationFn {
    private String fldname;
    private Constant val;

    public MaxFn(String fldname) {
        this.fldname = fldname;
    }

    @Override
    public String fieldName() {
        return "maxof" + fldname;
    }

    @Override
    public void processFirst(Scan s) throws LockAbortException {
        val = s.getVal(fldname);
    }

    @Override
    public void processNext(Scan s) throws LockAbortException {
        Constant newval = s.getVal(fldname);
        if (newval.compareTo(val) > 0)
            val = newval;
    }

    @Override
    public Constant value() {
        return val;
    }
}
