package com.simpledb.plan;

import com.simpledb.scan.Constant;
import com.simpledb.scan.Scan;

public interface AggregationFn {
    public String fieldName();
    public void processFirst(Scan s);
    public void processNext(Scan s);
    public Constant value();
}
