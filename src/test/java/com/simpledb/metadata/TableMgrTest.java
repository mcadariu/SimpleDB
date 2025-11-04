package com.simpledb.metadata;

import com.simpledb.buffer.BufferMgr;
import com.simpledb.file.FileMgr;
import com.simpledb.log.LogMgr;
import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.transaction.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.*;

public class TableMgrTest {
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
    public void testCreateTableAndGetLayout() {
        Schema schema = new Schema();
        Transaction tx = new Transaction(fileMgr, logMgr, bufferMgr);

        TableMgr tableMgr = new TableMgr(true, tx);
        schema.addIntField("A");
        schema.addStringField("B", 9);
        tableMgr.createTable("MyTable", schema, tx);

        Layout layout = tableMgr.getLayout("MyTable", tx);
        assertNotNull(layout);

        int size = layout.slotsize();
        assertTrue(size > 0);

        Schema schema2 = layout.schema();
        assertNotNull(schema2);

        // Verify fields
        int fieldCount = 0;
        for (String fldname : schema2.fields()) {
            fieldCount++;
            if ("A".equals(fldname)) {
                assertEquals(Types.INTEGER, schema2.type(fldname));
            } else if ("B".equals(fldname)) {
                assertEquals(Types.VARCHAR, schema2.type(fldname));
                assertEquals(9, schema2.length(fldname));
            }
        }
        assertEquals(2, fieldCount);

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
}
