package com.simpledb.concurrency;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.buffer.BufferMgr;
import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.log.LogMgr;
import com.simpledb.transaction.Transaction;

import java.io.File;

public class ConcurrencyTest {
    private static FileMgr fileMgr;
    private static LogMgr logMgr;
    private static BufferMgr bufferMgr;

    public static void main(String[] args) {
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "simpledb_test_" + System.currentTimeMillis());
        fileMgr = new FileMgr(tempDir, 400);
        logMgr = new LogMgr(fileMgr, "logtest");
        bufferMgr = new BufferMgr(fileMgr, logMgr, 3);

        A a = new A();
        new Thread(a).start();
        B b = new B();
        new Thread(b).start();
        C c = new C();
        new Thread(c).start();
    }

    static class A implements Runnable {
        public void run() {
            try {
                Transaction txA = new Transaction(fileMgr, logMgr, bufferMgr);
                BlockId blockId1 = new BlockId("testfile", 1);
                BlockId blockId2 = new BlockId("testfile", 2);

                txA.pin(blockId1);
                txA.pin(blockId2);

                System.out.println("Tx A: request slock 1");
                txA.getInt(blockId1, 0);
                System.out.println("Tx A: receive slock 1");
                Thread.sleep(1000);
                System.out.println("Tx A: request slock 2");
                txA.getString(blockId2, 0);
                System.out.println("Tx A: receive slock 2");
                txA.commit();
            } catch (InterruptedException | BufferAbortException | LockAbortException e) {

            }
        }
    }

    static class B implements Runnable {
        public void run() {
            try {
                Transaction txB = new Transaction(fileMgr, logMgr, bufferMgr);
                BlockId blockId1 = new BlockId("testfile", 1);
                BlockId blockId2 = new BlockId("testfile", 2);

                txB.pin(blockId1);
                txB.pin(blockId2);

                System.out.println("Tx B: request xlock 2");
                txB.setInt(blockId2, 0, 0, false);
                System.out.println("Tx B: receive xlock 2");
                Thread.sleep(1000);
                System.out.println("Tx B: request slock 1");
                txB.getInt(blockId1, 0);
                System.out.println("Tx B: receive slock 2");
                txB.commit();
            } catch (InterruptedException | BufferAbortException | LockAbortException e) {

            }
        }
    }

    static class C implements Runnable {
        public void run() {
            try {
                Transaction txC = new Transaction(fileMgr, logMgr, bufferMgr);
                BlockId blockId1 = new BlockId("testfile", 1);
                BlockId blockId2 = new BlockId("testfile", 2);

                txC.pin(blockId1);
                txC.pin(blockId2);

                System.out.println("Tx C: request xlock 1");
                txC.setInt(blockId1, 0, 0, false);
                System.out.println("Tx C: receive xlock 1");
                Thread.sleep(1000);
                System.out.println("Tx C: request slock 2");
                txC.getInt(blockId2, 0);
                System.out.println("Tx C: receive slock 2");
                txC.commit();
            } catch (InterruptedException | BufferAbortException | LockAbortException e) {

            }
        }
//        }

    }
}
