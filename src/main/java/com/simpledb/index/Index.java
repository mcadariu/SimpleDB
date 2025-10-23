package com.simpledb.index;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.scan.Constant;
import com.simpledb.scan.RID;

public interface Index {
    public void beforeFirst(Constant searchKey) throws BufferAbortException, LockAbortException;

    public boolean next() throws BufferAbortException, LockAbortException;

    public RID getDataRid() throws LockAbortException;

    public void insert(Constant dataVal, RID dataRid) throws BufferAbortException, LockAbortException;

    public void delete(Constant dataval, RID datarid) throws BufferAbortException, LockAbortException;

    public void close();
}
