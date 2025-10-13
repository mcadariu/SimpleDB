package com.simpledb.metadata;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.buffer.BufferMgr;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.file.FileMgr;
import com.simpledb.log.LogMgr;
import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.scan.TableScan;
import com.simpledb.transaction.Transaction;

import java.io.File;

public class CatalogTest {
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

        System.out.println("All tables and their lengths:");
        Layout layout = tableMgr.getLayout("tblcat", tx);
        TableScan ts = new TableScan(tx, "tblcat", layout);

        while (ts.next()) {
            String tname = ts.getString("tblname");
            int size = ts.getInt("slotsize");
            System.out.println(tname + " " + size);
        }

        ts.close();

        System.out.println("All fields and their offsets: ");
        layout = tableMgr.getLayout("fldcat", tx);
        ts = new TableScan(tx, "fldcat", layout);

        while (ts.next()) {
            String tname = ts.getString("tblname");
            String fname = ts.getString("fldname");
            int offset = ts.getInt("offset");
            System.out.println(tname + " " + fname + " " + offset);
        }

        ts.close();
    }
}
