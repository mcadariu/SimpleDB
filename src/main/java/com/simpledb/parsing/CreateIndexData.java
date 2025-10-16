package com.simpledb.parsing;

public class CreateIndexData {
    private String idxName, tblname, fldname;

    public CreateIndexData(String idxname, String tblname, String fldname) {
        this.idxName = idxname;
        this.tblname = tblname;
        this.fldname = fldname;
    }

    public String indexName() {
        return idxName;
    }

    public String tableName() {
        return tblname;
    }

    public String fieldName() {
        return fldname;
    }
}
