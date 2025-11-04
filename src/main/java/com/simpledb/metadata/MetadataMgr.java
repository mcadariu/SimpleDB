package com.simpledb.metadata;

import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.transaction.Transaction;

import java.util.Map;

public class MetadataMgr {
    private static TableMgr tableMgr;
    private static StatMgr statMgr;
    private static IndexMgr indexMgr;

    public MetadataMgr(boolean isnew, Transaction tx) {
        tableMgr = new TableMgr(isnew, tx);
        statMgr = new StatMgr(tableMgr, tx);
        indexMgr = new IndexMgr(isnew, tableMgr, statMgr, tx);
    }

    public void createTable(String tblname, Schema schema, Transaction tx) {
        tableMgr.createTable(tblname, schema, tx);
    }

    public Layout getLayout(String tblname, Transaction tx) {
        return tableMgr.getLayout(tblname, tx);
    }

    public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
        indexMgr.createIndex(idxname, tblname, fldname, tx);
    }

    public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
        return indexMgr.getIndexInfo(tblname, tx);
    }

    public StatInfo getStatInfo(String tblname, Layout layout, Transaction tx) {
        return statMgr.getStatInfo(tblname, layout, tx);
    }

}
