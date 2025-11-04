package com.simpledb.parse;

import com.simpledb.scan.Predicate;

import java.util.Collection;
import java.util.List;

public record QueryData(List<String> fields, Collection<String> tables, Predicate pred) {
    @Override
    public String toString() {
        String result = "select ";
        for (String fldname : fields)
            result += fldname + ", ";
        result = result.substring(0, result.length() - 2);
        result += "from ";
        for (String tblname : tables)
            result += tblname + ", ";
        result = result.substring(0, result.length() - 2);
        String predString = pred.toString();
        if (!predString.equals(""))
            result += " where " + predString;
        return result;
    }
}
