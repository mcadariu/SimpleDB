package com.simpledb.scan;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.buffer.BufferMgr;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.file.FileMgr;
import com.simpledb.log.LogMgr;
import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.transaction.Transaction;

import java.io.File;

public class TableScanTest {
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

        schema.addIntField("A");
        schema.addStringField("B", 9);

        Layout layout = new Layout(schema);

        for (String fldname : layout.schema().fields()) {
            int offset = layout.offset(fldname);
            System.out.println(fldname + " has offset " + offset);
        }

        TableScan tableScan = new TableScan(tx, "T", layout);

        System.out.println("Filling the table with 50 random records");
        tableScan.beforeFirst();

        for (int i = 0; i < 50; i++) {
            tableScan.insert();
            int n = (int) Math.round(Math.random() * 50);
            tableScan.setInt("A", n);
            tableScan.setString("B", "rec" + n);
            System.out.println("inserting into slot " + tableScan.getRid() + ": {" + n + ", " + "rec" + n + "}");
        }

        System.out.println("Deleting records with A-values < 25.");

        int count = 0;
        tableScan.beforeFirst();

        while (tableScan.next()) {
            int a = tableScan.getInt("A");
            String b = tableScan.getString("B");

            if (a < 25) {
                count++;
                System.out.println("slot " + tableScan.getRid() + ": {" + a + ", " + b + "}");
                tableScan.delete();
            }
        }

        System.out.println(count + " values under 10 were deleted. \n");
        System.out.println("Here are the remaining records.");

        tableScan.beforeFirst();

        while (tableScan.next()) {
            int a = tableScan.getInt("A");
            String b = tableScan.getString("B");
            System.out.println("slot " + tableScan.getRid() + ": {" + a + ", " + b + "}");
        }

        tableScan.close();
        tx.commit();
    }
}
