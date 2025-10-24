package com.simpledb.scan;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.file.TempTable;
import com.simpledb.plan.RecordComparator;

import java.util.Arrays;
import java.util.List;

public class SortScan implements Scan {
    private UpdateScan s1, s2 = null, currentScan = null;
    private RecordComparator comp;
    private boolean hasmore1, hasmore2 = false;
    private List<RID> savePosition;

    public SortScan(List<TempTable> runs, RecordComparator comp) throws BufferAbortException, LockAbortException {
        this.comp = comp;
        s1 = runs.get(0).open();
        hasmore1 = s1.next();
        if (runs.size() > 1) {
            s2 = runs.get(1).open();
            hasmore2 = s2.next();
        }
    }

    @Override
    public void beforeFirst() throws BufferAbortException, LockAbortException {
        s1.beforeFirst();
        hasmore1 = s1.next();
        if (s2 != null) {
            s2.beforeFirst();
            hasmore2 = s2.next();
        }
    }

    @Override
    public boolean next() throws LockAbortException, BufferAbortException {
        if (currentScan == s1)
            hasmore1 = s1.next();
        else if (currentScan == s2)
            hasmore2 = s2.next();

        if (!hasmore1 && !hasmore2)
            return false;
        else if (hasmore1 && hasmore2) {
            if (comp.compare(s1, s2) < 0)
                currentScan = s1;
            else
                currentScan = s2;
        } else if (hasmore1)
            currentScan = s1;
        else if (hasmore2)
            currentScan = s2;
        return true;
    }

    public void savePosition() {
        RID rid1 = s1.getRid();
        RID rid2 = s2.getRid();
        savePosition = Arrays.asList(rid1, rid2);
    }

    public void restorePosition() throws BufferAbortException {
        RID rid1 = savePosition.get(0);
        RID rid2 = savePosition.get(1);
        s1.moveToRid(rid1);
        s2.moveToRid(rid2);
    }

    @Override
    public int getInt(String fldname) throws LockAbortException {
        return currentScan.getInt(fldname);
    }

    @Override
    public String getString(String fldname) throws LockAbortException {
        return currentScan.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) throws LockAbortException {
        return currentScan.getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return false;
    }

    @Override
    public void close() {
        s1.close();
        if (s2 != null)
            s2.close();
    }
}
