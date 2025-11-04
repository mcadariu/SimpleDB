package com.simpledb.scan;

import com.simpledb.buffer.BufferMgr;
import com.simpledb.file.FileMgr;
import com.simpledb.log.LogMgr;
import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.transaction.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class TableScanTest {
    private FileMgr fileMgr;
    private LogMgr logMgr;
    private BufferMgr bufferMgr;
    private File tempDir;

    @Before
    public void setUp() {
        tempDir = new File(System.getProperty("java.io.tmpdir"), "simpledb_test_" + System.currentTimeMillis());
        fileMgr = new FileMgr(tempDir, 400);
        logMgr = new LogMgr(fileMgr, "logtest");
        bufferMgr = new BufferMgr(fileMgr, logMgr, 3);
    }

    @After
    public void tearDown() {
        if (tempDir != null && tempDir.exists()) {
            deleteDirectory(tempDir);
        }
    }

    @Test
    public void testTableScanInsertAndDelete() {
        Schema schema = new Schema();
        Transaction tx = new Transaction(fileMgr, logMgr, bufferMgr);

        schema.addIntField("A");
        schema.addStringField("B", 9);

        Layout layout = new Layout(schema);

        // Verify layout has both fields
        int fieldCount = 0;
        for (String fldname : layout.schema().fields()) {
            fieldCount++;
            int offset = layout.offset(fldname);
            assertTrue(offset >= 0);
        }
        assertEquals(2, fieldCount);

        TableScan tableScan = new TableScan(tx, "T", layout);

        // Insert 50 random records
        tableScan.beforeFirst();
        for (int i = 0; i < 50; i++) {
            tableScan.insert();
            int n = (int) Math.round(Math.random() * 50);
            tableScan.setInt("A", n);
            tableScan.setString("B", "rec" + n);
            assertNotNull(tableScan.getRid());
        }

        // Count and delete records with A < 25
        int count = 0;
        tableScan.beforeFirst();

        while (tableScan.next()) {
            int a = tableScan.getInt("A");
            String b = tableScan.getString("B");
            assertNotNull(b);

            if (a < 25) {
                count++;
                tableScan.delete();
            }
        }

        assertTrue(count > 0);

        // Verify remaining records all have A >= 25
        tableScan.beforeFirst();
        int remainingCount = 0;
        while (tableScan.next()) {
            int a = tableScan.getInt("A");
            String b = tableScan.getString("B");
            assertTrue(a >= 25);
            assertNotNull(b);
            remainingCount++;
        }

        assertEquals(50 - count, remainingCount);

        tableScan.close();
        tx.commit();
    }

    @Test
    public void testTableScanFieldOffsets() {
        Schema schema = new Schema();
        schema.addIntField("A");
        schema.addStringField("B", 9);

        Layout layout = new Layout(schema);

        // Verify field offsets are valid
        assertTrue(layout.offset("A") >= 0);
        assertTrue(layout.offset("B") >= 0);
    }

    private void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }
}
