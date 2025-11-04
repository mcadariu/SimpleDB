package com.simpledb.buffer;

import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.file.Page;
import com.simpledb.log.LogMgr;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BufferTest {
    private File tempDir;
    private FileMgr fileMgr;
    private LogMgr logMgr;
    private BufferMgr bufferMgr;

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
    public void testBufferPinUnpinAndAvailability() {
        Buffer[] buffers = new Buffer[6];

        buffers[0] = bufferMgr.pin(new BlockId("testfile", 0));
        buffers[1] = bufferMgr.pin(new BlockId("testfile", 1));
        buffers[2] = bufferMgr.pin(new BlockId("testfile", 2));

        assertNotNull(buffers[0]);
        assertNotNull(buffers[1]);
        assertNotNull(buffers[2]);
        assertEquals(0, bufferMgr.available());

        bufferMgr.unpin(buffers[1]);
        buffers[1] = null;
        assertEquals(1, bufferMgr.available());

        buffers[3] = bufferMgr.pin(new BlockId("testfile", 0));
        buffers[4] = bufferMgr.pin(new BlockId("testfile", 1));

        assertNotNull(buffers[3]);
        assertNotNull(buffers[4]);
        assertEquals(0, bufferMgr.available());
    }

    @Test
    public void testBufferModificationAndFlush() {
        Buffer buff1 = bufferMgr.pin(new BlockId("testfile", 1));
        assertNotNull(buff1);

        Page p = buff1.contents();
        int n = p.getInt(80);
        p.setInt(80, n + 1);
        buff1.setModified(1, 0);

        bufferMgr.unpin(buff1);

        Buffer buff2 = bufferMgr.pin(new BlockId("testfile", 2));
        Buffer buff3 = bufferMgr.pin(new BlockId("testfile", 3));
        Buffer buff4 = bufferMgr.pin(new BlockId("testfile", 4));

        assertNotNull(buff2);
        assertNotNull(buff3);
        assertNotNull(buff4);

        bufferMgr.unpin(buff2);

        buff2 = bufferMgr.pin(new BlockId("testfile", 1));
        Page p2 = buff2.contents();
        int value = p2.getInt(80);
        assertEquals(n + 1, value);

        p2.setInt(80, 9999);
        buff2.setModified(1, 0);
        bufferMgr.unpin(buff2);

        // Pin block 5 to force eviction of block 1 (which should flush it)
        Buffer buff5 = bufferMgr.pin(new BlockId("testfile", 5));
        assertNotNull(buff5);

        // Unpin blocks 3 and 4 to free up buffers
        bufferMgr.unpin(buff3);
        bufferMgr.unpin(buff4);

        // Re-pin block 1 and read it back
        Buffer readBack = bufferMgr.pin(new BlockId("testfile", 1));
        assertNotNull(readBack);
        assertEquals(9999, readBack.contents().getInt(80));

        bufferMgr.unpin(readBack);
        bufferMgr.unpin(buff5);
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
