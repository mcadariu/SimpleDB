package com.simpledb.file;

import java.io.File;

public class FileMgrTest {

    public static void main(String [] args) {
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "simpledb_test_" + System.currentTimeMillis());

        FileMgr fileMgr = new FileMgr(tempDir, 400);

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

        System.out.println("offset " + pos1 + "contains" + p2.getString(pos1));
        System.out.println("offset " + pos2 + "contains" + p2.getInt(pos2));
    }

}