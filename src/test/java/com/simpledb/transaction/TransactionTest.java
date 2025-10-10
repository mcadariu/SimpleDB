package com.simpledb.transaction;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.buffer.BufferMgr;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.log.LogMgr;

import java.io.File;

public class TransactionTest {
    private static FileMgr fileMgr;
    private static LogMgr logMgr;
    private static BufferMgr bufferMgr;

    public static void main(String[] args) throws BufferAbortException, LockAbortException {
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "simpledb_test_" + System.currentTimeMillis());
        fileMgr = new FileMgr(tempDir, 400);
        logMgr = new LogMgr(fileMgr, "logtest");
        bufferMgr = new BufferMgr(fileMgr, logMgr, 3);

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

        System.out.println("Initial value at location 80 = " + ival);
        System.out.println("Initial value at location 40 = " + sval);

        int newival = ival + 1;
        String newsval = sval + "!";

        tx2.setInt(blockId, 80, newival, true);
        tx2.setString(blockId, 40, newsval, true);
        tx2.commit();

        Transaction tx3 = new Transaction(fileMgr, logMgr, bufferMgr);
        tx3.pin(blockId);

        System.out.println("new value at location 80 =" + tx3.getString(blockId, 80));
        System.out.println("new value at location 40 = " + tx3.getString(blockId, 40));
        tx3.setInt(blockId, 80, 9999, true);
        System.out.println("pre-rollback value at location 80 =" + tx3.getInt(blockId, 80));
        tx3.rollback();

        Transaction tx4 = new Transaction(fileMgr, logMgr, bufferMgr);
        tx4.pin(blockId);
        System.out.println("post-rollback at location 80 = " + tx4.getInt(blockId, 80));
        tx4.commit();
    }
}
