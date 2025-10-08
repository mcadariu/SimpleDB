package com.simpledb.log;

import com.simpledb.file.FileMgr;
import com.simpledb.file.Page;

import java.io.File;
import java.util.Iterator;

public class LogTest {
    private static LogMgr logMgr;
    private static FileMgr fileMgr;

    public static void main(String[] args) {
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "simpledb_test_" + System.currentTimeMillis());
        fileMgr = new FileMgr(tempDir, 400);
        logMgr = new LogMgr(fileMgr, "logtest");

        createRecords(1, 35);
        printLogRecords();
        createRecords(36, 70);
        logMgr.flush(65);
        printLogRecords();
    }

    private static void printLogRecords() {
        System.out.println("The log file now has: ");
        Iterator<byte[]> iter = logMgr.iterator();

        while (iter.hasNext()) {
            byte[] rec = iter.next();
            Page p = new Page(rec, fileMgr.arena());
            String s = p.getString(0);
            int npos = Page.maxLength(s.length());
            int val = p.getInt(npos);
            System.out.println("[" + s + ", " + val + "]");
        }
        System.out.println();
    }

    private static void createRecords(int start, int end) {
        System.out.println("Creating records: ");
        for (int i = start; i <= end; i++) {
            byte[] rec = createLogRecord("record" + i, i+100);
            int lsn = logMgr.append(rec);
            System.out.println(lsn + " ");
        }
        System.out.println();
    }

    private static byte[] createLogRecord(String s, int n) {
        int npos = Page.maxLength(s.length());
        byte[] b = new byte[npos + Integer.BYTES];
        Page p = new Page(b, fileMgr.arena());
        p.setString(0, s);
        p.setInt(npos, n);
        return p.toByteArray();
    }
}
