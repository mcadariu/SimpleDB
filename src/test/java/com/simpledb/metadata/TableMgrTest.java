package com.simpledb.metadata;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.buffer.BufferMgr;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.file.FileMgr;
import com.simpledb.log.LogMgr;
import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.transaction.Transaction;

import java.io.File;
import java.sql.Types;

public class TableMgrTest {
    private static FileMgr fileMgr;
    private static LogMgr logMgr;
    private static BufferMgr bufferMgr;

    public static void main(String[] args) throws BufferAbortException, LockAbortException {
        Schema schema = new Schema();
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "simpledb_test_" + System.currentTimeMillis());
        fileMgr = new FileMgr(tempDir, 400);
        logMgr = new LogMgr(fileMgr, "logtest");
        bufferMgr = new BufferMgr(fileMgr, logMgr, 3);

        Transaction tx = new Transaction(fileMgr, logMgr, bufferMgr);

        TableMgr tableMgr = new TableMgr(true, tx);
        schema.addIntField("A");
        schema.addStringField("B", 9);
        tableMgr.createTable("MyTable", schema, tx);

        Layout layout = tableMgr.getLayout("MyTable", tx);
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
        tx.commit();
    }
}
