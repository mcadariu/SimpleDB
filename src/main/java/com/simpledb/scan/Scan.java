package com.simpledb.scan;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;

public interface Scan {
    public void beforeFirst() throws BufferAbortException, LockAbortException;

    public boolean next() throws LockAbortException, BufferAbortException;

    public int getInt(String fldname) throws LockAbortException;

    public String getString(String fldname) throws LockAbortException;

    public Constant getVal(String fldname) throws LockAbortException;

    public boolean hasField(String fldname);

    public void close();
}
