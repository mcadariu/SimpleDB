package com.simpledb.scan;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.plan.AggregationFn;

import java.util.List;

public class GroupByScan implements Scan {
    private Scan s;
    private List<String> groupByFields;
    private List<AggregationFn> aggfns;
    private GroupValue groupVal;
    private boolean moregroups;

    public GroupByScan(Scan s, List<String> groupByFields, List<AggregationFn> aggregationFns) throws BufferAbortException, LockAbortException {
        this.s = s;
        this.groupByFields = groupByFields;
        this.aggfns = aggregationFns;
        beforeFirst();
    }


    @Override
    public void beforeFirst() throws BufferAbortException, LockAbortException {
        s.beforeFirst();
        moregroups = s.next();
    }

    @Override
    public boolean next() throws LockAbortException, BufferAbortException {
        if (!moregroups)
            return false;
        for (AggregationFn fn : aggfns)
            fn.processFirst(s);
        groupVal = new GroupValue(s, groupByFields);

        while (moregroups == s.next()) {
            GroupValue gv = new GroupValue(s, groupByFields);
            if (!groupByFields.equals(gv))
                break;
            for (AggregationFn fn : aggfns)
                fn.processNext(s);
        }
        return true;
    }

    @Override
    public int getInt(String fldname) throws LockAbortException {
        return getVal(fldname).asInt();
    }

    @Override
    public String getString(String fldname) throws LockAbortException {
        return getVal(fldname).asString();
    }

    @Override
    public Constant getVal(String fldname) throws LockAbortException {
        if (groupByFields.contains(fldname))
            return groupVal.getVal(fldname);
        for (AggregationFn fn : aggfns)
            if (fn.fieldName().equals(fldname))
                return fn.value();
        throw new RuntimeException("no field " + fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        if (groupByFields.contains(fldname))
            return true;
        for (AggregationFn fn : aggfns)
            if (fn.fieldName().equals(fldname))
                return true;
        return false;
    }

    @Override
    public void close() {
        s.close();
    }
}
