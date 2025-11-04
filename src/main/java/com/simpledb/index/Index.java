package com.simpledb.index;

import com.simpledb.scan.Constant;
import com.simpledb.scan.RID;

public interface Index {
    public void beforeFirst(Constant searchKey);

    public boolean next();

    public RID getDataRid();

    public void insert(Constant dataVal, RID dataRid);

    public void delete(Constant dataval, RID datarid);

    public void close();
}
