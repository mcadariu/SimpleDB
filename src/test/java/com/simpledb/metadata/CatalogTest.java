package com.simpledb.metadata;

import com.simpledb.buffer.BufferMgr;
import com.simpledb.file.FileMgr;
import com.simpledb.log.LogMgr;
import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.scan.TableScan;
import com.simpledb.transaction.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CatalogTest {
    private FileMgr fileMgr;
    private LogMgr logMgr;
    private BufferMgr bufferMgr;
    private File tempDir;

    @BeforeEach
    public void setUp() {
        tempDir = new File(System.getProperty("java.io.tmpdir"), "simpledb_test_" + System.currentTimeMillis());
        fileMgr = new FileMgr(tempDir, 400);
        logMgr = new LogMgr(fileMgr, "logtest");
        bufferMgr = new BufferMgr(fileMgr, logMgr, 3);
    }

    @AfterEach
    public void tearDown() {
        if (tempDir != null && tempDir.exists()) {
            deleteDirectory(tempDir);
        }
    }

    @Test
    public void testTableCatalog() {
        Schema schema = new Schema();
        Transaction tx = new Transaction(fileMgr, logMgr, bufferMgr);

        TableMgr tableMgr = new TableMgr(true, tx);
        schema.addIntField("A");
        schema.addStringField("B", 9);
        tableMgr.createTable("MyTable", schema, tx);

        // Read all tables from catalog
        Layout layout = tableMgr.getLayout("tblcat", tx);
        TableScan ts = new TableScan(tx, "tblcat", layout);

        List<String> tableNames = new ArrayList<>();
        while (ts.next()) {
            String tname = ts.getString("tblname");
            int size = ts.getInt("slotsize");
            tableNames.add(tname);
            assertTrue(size > 0);
        }
        ts.close();

        // Verify catalog tables exist
        assertTrue(tableNames.contains("tblcat"));
        assertTrue(tableNames.contains("fldcat"));
        assertTrue(tableNames.contains("MyTable"));

        tx.commit();
    }

    @Test
    public void testFieldCatalog() {
        Schema schema = new Schema();
        Transaction tx = new Transaction(fileMgr, logMgr, bufferMgr);

        TableMgr tableMgr = new TableMgr(true, tx);
        schema.addIntField("A");
        schema.addStringField("B", 9);
        tableMgr.createTable("MyTable", schema, tx);

        // Read all fields from catalog
        Layout layout = tableMgr.getLayout("fldcat", tx);
        TableScan ts = new TableScan(tx, "fldcat", layout);

        List<FieldInfo> myTableFields = new ArrayList<>();
        while (ts.next()) {
            String tname = ts.getString("tblname");
            String fname = ts.getString("fldname");
            int offset = ts.getInt("offset");

            if ("MyTable".equals(tname)) {
                myTableFields.add(new FieldInfo(fname, offset));
            }
        }
        ts.close();

        // Verify MyTable has two fields
        assertEquals(2, myTableFields.size());

        // Find field A and B
        boolean foundA = false;
        boolean foundB = false;
        for (FieldInfo field : myTableFields) {
            if ("A".equals(field.name)) {
                foundA = true;
            } else if ("B".equals(field.name)) {
                foundB = true;
            }
        }
        assertTrue(foundA);
        assertTrue(foundB);

        tx.commit();
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

    private static class FieldInfo {
        final String name;
        final int offset;

        FieldInfo(String name, int offset) {
            this.name = name;
            this.offset = offset;
        }
    }
}
