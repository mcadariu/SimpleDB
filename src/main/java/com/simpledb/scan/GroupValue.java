package com.simpledb.scan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupValue {
    private Map<String, Constant> vals = new HashMap<>();

    public GroupValue(Scan s, List<String> groupByFields) {
        for (String fldname : groupByFields)
            vals.put(fldname, s.getVal(fldname));
    }

    public Constant getVal(String fldname) {
        return vals.get(fldname);
    }

    public boolean equals(Object obj) {
        GroupValue gv = (GroupValue) obj;
        for (String fldname : vals.keySet()) {
            Constant v1 = vals.get(fldname);
            Constant v2 = gv.getVal(fldname);
            if (!v1.equals(v2))
                return false;
        }
        return true;
    }

    public int hasCode() {
        int hashval = 0;
        for (Constant c : vals.values())
            hashval += c.hashCode();
        return hashval;
    }
}
