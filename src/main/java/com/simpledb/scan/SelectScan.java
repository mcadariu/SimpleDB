package com.simpledb.scan;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;

public class SelectScan implements Scan {
    private Scan s;
    private Predicate pred;

    public SelectScan(Scan s, Predicate pred) {
        this.s = s;
        this.pred = pred;
    }

    public void beforeFirst() throws BufferAbortException, LockAbortException {
        s.beforeFirst();
    }

    public boolean next() throws BufferAbortException, LockAbortException {
        while (s.next())
            if (pred.isSatisfied(s))
                return true;
        return false;
    }

    public int getInt(String fldname) throws LockAbortException {
        return s.getInt(fldname);
    }

    public String getString(String fldname) throws LockAbortException {
        return s.getString(fldname);
    }

    public Constant getVal(String fldname) throws LockAbortException {
        return s.getVal(fldname);
    }

    public boolean hasField(String fldname) {
        return s.hasField(fldname);
    }

    public void close() {
        s.close();
    }
}
