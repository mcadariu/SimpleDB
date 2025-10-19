package com.simpledb.metadata;

import com.simpledb.SimpleDB;
import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.scan.TableScan;
import com.simpledb.transaction.Transaction;

import java.sql.Types;
import java.util.Map;

public class MetadataMgrTest {
    public static void main(String[] args) throws BufferAbortException, LockAbortException {
        SimpleDB simpleDB = new SimpleDB(400, 8);
        Transaction tx = simpleDB.newTransaction();
        MetadataMgr metadataMgr = new MetadataMgr(true, tx);

        Schema schema = new Schema();
        schema.addIntField("A");
        schema.addStringField("B", 9);

        metadataMgr.createTable("MyTable", schema, tx);
        Layout layout = metadataMgr.getLayout("MyTable", tx);

        int size = layout.slotsize();
        Schema schema2 = layout.schema();

        System.out.println("MyTable has slot size " + size);
        System.out.println("Its fields are: ");
        for (String fldname : schema2.fields()) {
            String type;

            if (schema2.type(fldname) == Types.INTEGER)
                type = "int";
            else {
                int strlen = schema2.length(fldname);
                type = "varchar(" + strlen + ")";
            }
            System.out.println(fldname + ": " + type);
        }

        TableScan ts = new TableScan(tx, "MyTable", layout);
        for (int i = 0; i < 50; i++) {
            ts.insert();
            int n = (int) Math.round(Math.random() * 50);
            ts.setInt("A", n);
            ts.setString("B", "rec" + n);
        }
        StatInfo si = metadataMgr.getStatInfo("MyTable", layout, tx);
        System.out.println("B(MyTable) = " + si.blocksAccessed());
        System.out.println("R(MyTable) = " + si.recordsOutput());
        System.out.println("V(MyTable, A) = " + si.distinctValues("A"));
        System.out.println("V(MyTable, B) = " + si.distinctValues("B"));

        metadataMgr.createIndex("indexA", "MyTable", "A", tx);
        metadataMgr.createIndex("indexB", "MyTable", "B", tx);
        Map<String, IndexInfo> idxmap = metadataMgr.getIndexInfo("MyTable", tx);

        IndexInfo ii = idxmap.get("A");
        System.out.println("B(indexA) = " + ii.blocksAccessed());
        System.out.println("R(indexA) = " + ii.recordsOutput());
        System.out.println("V(indexA, A) = " + ii.distinctValues("A"));
        System.out.println("V(indexA, B) = " + ii.distinctValues("B"));

        ii = idxmap.get("B");
        System.out.println("B(indexB) = " + ii.blocksAccessed());
        System.out.println("R(indexB) = " + ii.recordsOutput());
        System.out.println("V(indexB, A) = " + ii.distinctValues("A"));
        System.out.println("V(indexB, B) = " + ii.distinctValues("B"));

    }
}
