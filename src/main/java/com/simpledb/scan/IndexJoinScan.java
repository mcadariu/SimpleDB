package com.simpledb.scan;

import com.simpledb.index.Index;

public class IndexJoinScan implements Scan {
    private Scan lhs;
    private Index idx;
    private String joinField;
    private TableScan rhs;

    public IndexJoinScan(Scan s, Index idx, String joinField, TableScan ts) {
        this.lhs = s;
        this.idx = idx;
        this.joinField = joinField;
        this.rhs = ts;
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        lhs.beforeFirst();
        lhs.next();
        resetIndex();
    }

    @Override
    public boolean next() {
        while (true) {
            if (idx.next()) {
                rhs.moveToRid(idx.getDataRid());
                return true;
            }
            if (!lhs.next())
                return false;
            resetIndex();
        }
    }

    @Override
    public int getInt(String fldname) {
        if (rhs.hasField(fldname))
            return rhs.getInt(fldname);
        else
            return lhs.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        if (rhs.hasField(fldname))
            return rhs.getString(fldname);
        else
            return lhs.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) {
        if (rhs.hasField(fldname))
            return rhs.getVal(fldname);
        else
            return lhs.getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return rhs.hasField(fldname) || lhs.hasField(fldname);
    }

    @Override
    public void close() {
        lhs.close();
        idx.close();
        rhs.close();
    }

    private void resetIndex() {
        Constant searchkey = lhs.getVal(joinField);
        idx.beforeFirst(searchkey);
    }
}
