package com.simpledb.record;

import com.simpledb.buffer.BufferMgr;
import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.log.LogMgr;
import com.simpledb.transaction.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class RecordTest {
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
    public void testRecordPageInsertAndDelete() {
        Schema schema = new Schema();
        Transaction tx = new Transaction(fileMgr, logMgr, bufferMgr);

        schema.addIntField("A");
        schema.addStringField("B", 9);

        Layout layout = new Layout(schema);

        // Verify field offsets
        int fieldCount = 0;
        for (String fldname : layout.schema().fields()) {
            int offset = layout.offset(fldname);
            assertTrue(offset >= 0);
            fieldCount++;
        }
        assertEquals(2, fieldCount);

        BlockId blockId = tx.append("testfile");
        tx.pin(blockId);
        RecordPage recordPage = new RecordPage(tx, blockId, layout);
        recordPage.format();

        // Fill the page with random records
        int slot = recordPage.insertAfter(-1);
        int insertedCount = 0;

        while (slot >= 0) {
            int n = (int) Math.round(Math.random() * 50);
            recordPage.setInt(slot, "A", n);
            recordPage.setString(slot, "B", "rec" + n);
            insertedCount++;
            slot = recordPage.insertAfter(slot);
        }

        assertTrue(insertedCount > 0);

        // Delete records with A-values < 25
        int count = 0;
        slot = recordPage.nextAfter(-1);
        while (slot >= 0) {
            int a = recordPage.getInt(slot, "A");
            String b = recordPage.getString(slot, "B");
            assertNotNull(b);

            if (a < 25) {
                count++;
                recordPage.delete(slot);
            }
            slot = recordPage.nextAfter(slot);
        }

        assertTrue(count >= 0);

        // Verify remaining records all have A >= 25
        slot = recordPage.nextAfter(-1);
        int remainingCount = 0;
        while (slot >= 0) {
            int a = recordPage.getInt(slot, "A");
            String b = recordPage.getString(slot, "B");
            assertTrue(a >= 25);
            assertNotNull(b);
            remainingCount++;
            slot = recordPage.nextAfter(slot);
        }

        assertEquals(insertedCount - count, remainingCount);

        tx.unpin(blockId);
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
