package com.simpledb.plan;

import com.simpledb.scan.Constant;
import com.simpledb.scan.Scan;

import java.util.Collection;
import java.util.List;

public class RecordComparator {
    private Collection<String> fields;

    public RecordComparator(List<String> fields) {
        this.fields = fields;
    }

    public int compare(Scan src1, Scan src2) {
        for(String fldname: fields) {
            Constant val1 = src1.getVal(fldname);
            Constant val2 = src2.getVal(fldname);
            int result = val1.compareTo(val2);
            if (result != 0)
                return result;
        }
        return 0;
    }
}
