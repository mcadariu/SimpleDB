package com.simpledb.parse;

import com.simpledb.scan.Predicate;

import java.util.Collection;
import java.util.List;

public class QueryData {
    private List<String> fields;
    private Collection<String> tables;
    private Predicate pred;

    public QueryData(List<String> fields, Collection<String> tables, Predicate pred) {
        this.fields = fields;
        this.tables = tables;
        this.pred = pred;
    }

    public List<String> fields() {
        return fields;
    }

    public Collection<String> tables() {
        return tables;
    }

    public Predicate pred() {
        return pred;
    }

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
