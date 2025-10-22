package com.simpledb.scan;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;

public interface UpdateScan extends Scan {
    public void setInt(String fldname, int val) throws LockAbortException;
    public void setString(String fldname, String val) throws LockAbortException;
    public void setVal(String fldname, Constant val) throws LockAbortException;
    public void insert() throws LockAbortException, BufferAbortException;
    public void delete() throws LockAbortException;

    public RID getRid();
    public void moveToRid(RID rid) throws BufferAbortException;
}
