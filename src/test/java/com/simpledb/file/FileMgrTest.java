package com.simpledb.file;

import java.io.File;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileMgrTest {
    private File tempDir;
    private FileMgr fileMgr;

    @Before
    public void setUp() {
        tempDir = new File(System.getProperty("java.io.tmpdir"), "simpledb_test_" + System.currentTimeMillis());
        fileMgr = new FileMgr(tempDir, 400);
    }

    @After
    public void tearDown() {
        if (tempDir != null && tempDir.exists()) {
            deleteDirectory(tempDir);
        }
    }

    @Test
    public void testReadAndWriteBlock() {
        BlockId blk = new BlockId("yolo", 2);
        Page p1 = new Page(fileMgr.blockSize(), fileMgr.arena());
        int pos1 = 88;
        p1.setString(pos1, "abcyolo");
        int size = Page.maxLength("abcyolo".length());
        int pos2 = pos1 + size;
        p1.setInt(pos2, 69);

        fileMgr.write(blk, p1);

        Page p2 = new Page(fileMgr.blockSize(), fileMgr.arena());
        fileMgr.read(blk, p2);

        assertEquals("abcyolo", p2.getString(pos1));
        assertEquals(69, p2.getInt(pos2));
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