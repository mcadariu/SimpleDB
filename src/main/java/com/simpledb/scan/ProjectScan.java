package com.simpledb.scan;

import java.util.Collection;
import java.util.List;

public class ProjectScan implements Scan {

    private Scan s;
    private Collection<String> fieldlist;

    public ProjectScan(Scan s, List<String> fieldlist) {
        this.s = s;
        this.fieldlist = fieldlist;
    }

    @Override
    public void beforeFirst() {
        s.beforeFirst();
        ;
    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public int getInt(String fldname) {
        if (hasField(fldname))
            return s.getInt(fldname);
        else throw new RuntimeException("field not found.");
    }

    @Override
    public String getString(String fldname) {
        if (hasField(fldname))
            return s.getString(fldname);
        else throw new RuntimeException("field not found.");
    }

    @Override
    public Constant getVal(String fldname) {
        if (hasField(fldname))
            return s.getVal(fldname);
        else throw new RuntimeException("field not found.");
    }

    @Override
    public boolean hasField(String fldname) {
        return fieldlist.contains(fldname);
    }

    @Override
    public void close() {
        s.close();
    }
}
