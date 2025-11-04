package com.simpledb.transaction;

import com.simpledb.buffer.BufferMgr;
import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.log.LogMgr;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class TransactionTest {
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
    public void testTransactionCommit() {
        Transaction tx1 = new Transaction(fileMgr, logMgr, bufferMgr);
        BlockId blockId = new BlockId("testfile", 1);
        tx1.pin(blockId);

        tx1.setInt(blockId, 80, 1, false);
        tx1.setString(blockId, 40, "one", false);
        tx1.commit();

        Transaction tx2 = new Transaction(fileMgr, logMgr, bufferMgr);
        tx2.pin(blockId);

        int ival = tx2.getInt(blockId, 80);
        String sval = tx2.getString(blockId, 40);

        assertEquals(1, ival);
        assertEquals("one", sval);

        int newival = ival + 1;
        String newsval = sval + "!";

        tx2.setInt(blockId, 80, newival, true);
        tx2.setString(blockId, 40, newsval, true);
        tx2.commit();

        // Verify changes were committed
        Transaction tx3 = new Transaction(fileMgr, logMgr, bufferMgr);
        tx3.pin(blockId);

        assertEquals(2, tx3.getInt(blockId, 80));
        assertEquals("one!", tx3.getString(blockId, 40));

        tx3.commit();
    }

    @Test
    public void testTransactionRollback() {
        // Set up initial values
        Transaction tx1 = new Transaction(fileMgr, logMgr, bufferMgr);
        BlockId blockId = new BlockId("testfile", 1);
        tx1.pin(blockId);
        tx1.setInt(blockId, 80, 100, false);
        tx1.setString(blockId, 40, "initial", false);
        tx1.commit();

        // Modify and rollback
        Transaction tx2 = new Transaction(fileMgr, logMgr, bufferMgr);
        tx2.pin(blockId);

        assertEquals(100, tx2.getInt(blockId, 80));

        tx2.setInt(blockId, 80, 9999, true);
        assertEquals(9999, tx2.getInt(blockId, 80));

        tx2.rollback();

        // Verify rollback restored original values
        Transaction tx3 = new Transaction(fileMgr, logMgr, bufferMgr);
        tx3.pin(blockId);
        assertEquals(100, tx3.getInt(blockId, 80));
        assertEquals("initial", tx3.getString(blockId, 40));
        tx3.commit();
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
