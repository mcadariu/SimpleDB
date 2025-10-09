package com.simpledb.buffer;

import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.file.Page;
import com.simpledb.log.LogMgr;

import java.io.File;

public class BufferTest {
    public static void main(String[] args) throws BufferAbortException {
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "simpledb_test_" + System.currentTimeMillis());

        FileMgr fileMgr = new FileMgr(tempDir, 400);
        LogMgr logMgr = new LogMgr(fileMgr, "logtest");
        BufferMgr bufferMgr = new BufferMgr(fileMgr, logMgr, 3);

        bufferTest(bufferMgr);
//        bufferMgrTest(bufferMgr);
    }

    private static void bufferMgrTest(BufferMgr bufferMgr) throws BufferAbortException {
        Buffer[] buffers = new Buffer[6];

        buffers[0] = bufferMgr.pin(new BlockId("testfile", 0));
        buffers[1] = bufferMgr.pin(new BlockId("testfile", 1));
        buffers[2] = bufferMgr.pin(new BlockId("testfile", 2));

        bufferMgr.unpin(buffers[1]);
        buffers[1] = null;

        buffers[3] = bufferMgr.pin(new BlockId("testfile", 0));
        buffers[4] = bufferMgr.pin(new BlockId("testfile", 1));
        System.out.println("Available buffers: " + bufferMgr.available());
    }

    private static void bufferTest(BufferMgr bufferMgr) throws BufferAbortException {
        Buffer buff1 = bufferMgr.pin(new BlockId("testfile", 1));

        Page p = buff1.contents();

        int n = p.getInt(80);
        p.setInt(80, n + 1);

        buff1.setModified(1, 0);

        System.out.println("the new value is: " + (n + 1));

        bufferMgr.unpin(buff1);

        Buffer buff2 = bufferMgr.pin(new BlockId("testfile", 2));
        Buffer buff3 = bufferMgr.pin(new BlockId("testfile", 3));
        Buffer buff4 = bufferMgr.pin(new BlockId("testfile", 4));
        System.out.println("Pinned block 4 - block 1 was automatically flushed");

        bufferMgr.unpin(buff2);

        buff2 = bufferMgr.pin(new BlockId("testfile", 1));
        Page p2 = buff2.contents();
        System.out.println("Block 1 value (should be 1): " + p2.getInt(80));
        p2.setInt(80, 9999);
        buff2.setModified(1, 0);
        System.out.println("Set block 1 to 9999");
        bufferMgr.unpin(buff2);

        // Pin block 5 to force eviction of block 1 (which should flush it)
        Buffer buff5 = bufferMgr.pin(new BlockId("testfile", 5));
        System.out.println("Pinned block 5 - block 1 should be flushed now");

        // Unpin blocks 3 and 4 to free up buffers
        bufferMgr.unpin(buff3);
        bufferMgr.unpin(buff4);

        // Re-pin block 1 and read it back
        Buffer readBack = bufferMgr.pin(new BlockId("testfile", 1));
        System.out.println("Block 1 value after flush (should be 9999): " + readBack.contents().getInt(80));
        bufferMgr.unpin(readBack);
        bufferMgr.unpin(buff5);
    }
}
