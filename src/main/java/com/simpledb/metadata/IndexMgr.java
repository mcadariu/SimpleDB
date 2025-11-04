package com.simpledb.metadata;

import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.scan.TableScan;
import com.simpledb.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

import static com.simpledb.metadata.TableMgr.MAX_NAME;

public class IndexMgr {
    private Layout layout;
    private TableMgr tableMgr;
    private StatMgr statMgr;

    public IndexMgr(boolean isnew, TableMgr tableMgr, StatMgr statMgr, Transaction tx) {
        if (isnew) {
            Schema schema = new Schema();
            schema.addStringField("indexname", MAX_NAME);
            schema.addStringField("tablename", MAX_NAME);
            schema.addStringField("fieldname", MAX_NAME);
            tableMgr.createTable("idxcat", schema, tx);
        }

        this.tableMgr = tableMgr;
        this.statMgr = statMgr;
        layout = tableMgr.getLayout("idxcat", tx);
    }

    public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
        TableScan tableScan = new TableScan(tx, "idxcat", layout);
        tableScan.insert();

        tableScan.setString("indexname", idxname);
        tableScan.setString("tablename", tblname);
        tableScan.setString("fieldname", fldname);
        tableScan.close();
    }

    public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
        Map<String, IndexInfo> result = new HashMap<>();
        TableScan tableScan = new TableScan(tx, "idxcat", layout);

        while (tableScan.next()) {
            if (tableScan.getString("tablename").equals(tblname)) {
                String idxname = tableScan.getString("indexname");
                String fldname = tableScan.getString("fieldname");

                Layout tblLayout = tableMgr.getLayout(tblname, tx);
                StatInfo tblsi = statMgr.getStatInfo(tblname, tblLayout, tx);
                IndexInfo ii = new IndexInfo(idxname, fldname, tblLayout.schema(), tx, tblsi);
                result.put(fldname, ii);
            }
        }
        tableScan.close();
        return result;
    }
}
