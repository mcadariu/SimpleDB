package com.simpledb.scan;

public interface UpdateScan extends Scan {
    public void setInt(String fldname, int val);
    public void setString(String fldname, String val);
    public void setVal(String fldname, Constant val);
    public void insert();
    public void delete();

    public RID getRid();
    public void moveToRid(RID rid);
}
