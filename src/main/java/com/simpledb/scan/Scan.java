package com.simpledb.scan;

public interface Scan {
    public void beforeFirst();

    public boolean next();

    public int getInt(String fldname);

    public String getString(String fldname);

    public Constant getVal(String fldname);

    public boolean hasField(String fldname);

    public void close();
}
