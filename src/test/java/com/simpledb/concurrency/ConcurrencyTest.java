package com.simpledb.concurrency;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.buffer.BufferMgr;
import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.log.LogMgr;
import com.simpledb.transaction.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConcurrencyTest {
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
    public void testConcurrentTransactions() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        AtomicBoolean success = new AtomicBoolean(true);

        Thread threadA = new Thread(new TransactionA(latch, success));
        Thread threadB = new Thread(new TransactionB(latch, success));
        Thread threadC = new Thread(new TransactionC(latch, success));

        threadA.start();
        threadB.start();
        threadC.start();

        // Wait for all threads to complete (with timeout)
        latch.await();

        assertTrue(success.get(), "All transactions should complete successfully");
    }

    class TransactionA implements Runnable {
        private final CountDownLatch latch;
        private final AtomicBoolean success;

        TransactionA(CountDownLatch latch, AtomicBoolean success) {
            this.latch = latch;
            this.success = success;
        }

        public void run() {
            try {
                Transaction txA = new Transaction(fileMgr, logMgr, bufferMgr);
                BlockId blockId1 = new BlockId("testfile", 1);
                BlockId blockId2 = new BlockId("testfile", 2);

                txA.pin(blockId1);
                txA.pin(blockId2);

                txA.getInt(blockId1, 0);
                Thread.sleep(1000);
                txA.getString(blockId2, 0);
                txA.commit();
            } catch (InterruptedException | BufferAbortException | LockAbortException e) {
                success.set(false);
            } finally {
                latch.countDown();
            }
        }
    }

    class TransactionB implements Runnable {
        private final CountDownLatch latch;
        private final AtomicBoolean success;

        TransactionB(CountDownLatch latch, AtomicBoolean success) {
            this.latch = latch;
            this.success = success;
        }

        public void run() {
            try {
                Transaction txB = new Transaction(fileMgr, logMgr, bufferMgr);
                BlockId blockId1 = new BlockId("testfile", 1);
                BlockId blockId2 = new BlockId("testfile", 2);

                txB.pin(blockId1);
                txB.pin(blockId2);

                txB.setInt(blockId2, 0, 0, false);
                Thread.sleep(1000);
                txB.getInt(blockId1, 0);
                txB.commit();
            } catch (InterruptedException | BufferAbortException | LockAbortException e) {
                success.set(false);
            } finally {
                latch.countDown();
            }
        }
    }

    class TransactionC implements Runnable {
        private final CountDownLatch latch;
        private final AtomicBoolean success;

        TransactionC(CountDownLatch latch, AtomicBoolean success) {
            this.latch = latch;
            this.success = success;
        }

        public void run() {
            try {
                Transaction txC = new Transaction(fileMgr, logMgr, bufferMgr);
                BlockId blockId1 = new BlockId("testfile", 1);
                BlockId blockId2 = new BlockId("testfile", 2);

                txC.pin(blockId1);
                txC.pin(blockId2);

                txC.setInt(blockId1, 0, 0, false);
                Thread.sleep(1000);
                txC.getInt(blockId2, 0);
                txC.commit();
            } catch (InterruptedException | BufferAbortException | LockAbortException e) {
                success.set(false);
            } finally {
                latch.countDown();
            }
        }
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
