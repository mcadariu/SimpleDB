package com.simpledb.metadata;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.scan.TableScan;
import com.simpledb.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class TableMgr {
    public static final int MAX_NAME = 16; // table of field name
    private Layout tcatLayout, fcatLayout;

    public TableMgr(boolean isNew, Transaction tx) throws BufferAbortException, LockAbortException {
        Schema tcatSchema = new Schema();
        tcatSchema.addStringField("tblname", MAX_NAME);
        tcatSchema.addIntField("slotsize");

        tcatLayout = new Layout(tcatSchema);
        Schema fcatSchema = new Schema();
        fcatSchema.addStringField("tblname", MAX_NAME);
        fcatSchema.addStringField("fldname", MAX_NAME);
        fcatSchema.addIntField("type");
        fcatSchema.addIntField("length");
        fcatSchema.addIntField("offset");
        fcatLayout = new Layout(fcatSchema);

        if (isNew) {
            createTable("tblcat", tcatSchema, tx);
            createTable("fldcat", fcatSchema, tx);
        }

    }

    public void createTable(String tblname, Schema sch, Transaction tx) throws BufferAbortException, LockAbortException {
        Layout layout = new Layout(sch);

        TableScan tcat = new TableScan(tx, "tblcat", tcatLayout);
        tcat.insert();
        tcat.setString("tblname", tblname);
        tcat.setInt("slotsize", layout.slotsize());
        tcat.close();

        TableScan fcat = new TableScan(tx, "fldcat", fcatLayout);
        for (String fldname : sch.fields()) {
            fcat.insert();
            fcat.setString("tblname", tblname);
            fcat.setString("fldname", fldname);
            fcat.setInt("type", sch.type(fldname));
            fcat.setInt("length", sch.length(fldname));
            fcat.setInt("offset", layout.offset(fldname));
        }
        fcat.close();
    }

    public Layout getLayout(String tblname, Transaction tx) throws BufferAbortException, LockAbortException {
        int size = -1;
        TableScan tcat = new TableScan(tx, "tblcat", tcatLayout);
        while (tcat.next()) {
            if (tcat.getString("tblname").equals(tblname)) {
                size = tcat.getInt("slotsize");
                break;
            }
        }
        tcat.close();

        Schema schema = new Schema();
        Map<String, Integer> offsets = new HashMap<String, Integer>();
        TableScan fcat = new TableScan(tx, "fldcat", fcatLayout);
        while (fcat.next()) {
            if (fcat.getString("tblname").equals(tblname)) {
                String fldname = fcat.getString("fldname");
                int fldtype = fcat.getInt("type");
                int fldlen = fcat.getInt("length");
                int offset = fcat.getInt("offset");
                offsets.put(fldname, offset);
                schema.addField(fldname, fldtype, fldlen);
            }
        }
        fcat.close();
        return new Layout(schema, offsets, size);
    }
}
