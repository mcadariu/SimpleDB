package com.simpledb.metadata;

import com.simpledb.index.HashIndex;
import com.simpledb.index.Index;
import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.transaction.Transaction;

import java.sql.Types;

public class IndexInfo {
    private String idxname, fldname;
    private Transaction tx;
    private Schema tblSchema;
    private Layout idxLayout;
    private StatInfo statInfo;

    public IndexInfo(String idxname, String fldname, Schema tblSchema, Transaction tx, StatInfo statInfo) {
        this.idxname = idxname;
        this.fldname = fldname;
        this.tblSchema = tblSchema;
        this.tx = tx;
        this.idxLayout = createIdxLayout();
        this.statInfo = statInfo;
    }

    public int blocksAccessed() {
        int rpb = tx.blockSize();
        int numblocks = statInfo.recordsOutput() / rpb;
        return HashIndex.searchCost(numblocks, rpb);
    }

    public int recordsOutput() {
        return statInfo.recordsOutput() / statInfo.distinctValues(fldname);
    }

    public int distinctValues(String fname) {
        return fldname.equals(fname) ? 1 : statInfo.distinctValues(fldname);
    }

    private Layout createIdxLayout() {
        Schema schema = new Schema();
        schema.addIntField("block");
        schema.addIntField("id");
        if (tblSchema.type(fldname) == Types.INTEGER)
            schema.addIntField("dataval");
        else {
            int fldlen = tblSchema.length(fldname);
            schema.addStringField("dataval", fldlen);
        }

        return new Layout(schema);
    }

    public Index open() {
        Schema sch = tblSchema;
        return new HashIndex(tx, idxname, idxLayout);
    }

}
