package com.simpledb.log;

import com.simpledb.file.FileMgr;
import com.simpledb.file.Page;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class LogTest {
    private LogMgr logMgr;
    private FileMgr fileMgr;
    private File tempDir;

    @Before
    public void setUp() {
        tempDir = new File(System.getProperty("java.io.tmpdir"), "simpledb_test_" + System.currentTimeMillis());
        fileMgr = new FileMgr(tempDir, 400);
        logMgr = new LogMgr(fileMgr, "logtest");
    }

    @After
    public void tearDown() {
        if (tempDir != null && tempDir.exists()) {
            deleteDirectory(tempDir);
        }
    }

    @Test
    public void testCreateAndReadLogRecords() {
        // Create first batch of records
        createRecords(1, 35);
        List<LogRecord> records1 = readLogRecords();
        assertEquals(35, records1.size());

        // Verify first record
        LogRecord firstRecord = records1.get(records1.size() - 1);
        assertEquals("record1", firstRecord.text);
        assertEquals(101, firstRecord.value);

        // Create second batch and flush
        createRecords(36, 70);
        logMgr.flush(65);
        List<LogRecord> records2 = readLogRecords();
        assertEquals(70, records2.size());

        // Verify last record
        LogRecord lastRecord = records2.get(0);
        assertEquals("record70", lastRecord.text);
        assertEquals(170, lastRecord.value);
    }

    @Test
    public void testLogRecordOrder() {
        createRecords(1, 10);
        List<LogRecord> records = readLogRecords();

        // Log records should be in reverse order
        assertEquals(10, records.size());
        assertEquals("record10", records.get(0).text);
        assertEquals("record1", records.get(9).text);
    }

    private List<LogRecord> readLogRecords() {
        List<LogRecord> records = new ArrayList<>();
        Iterator<byte[]> iter = logMgr.iterator();

        while (iter.hasNext()) {
            byte[] rec = iter.next();
            Page p = new Page(rec, fileMgr.arena());
            String s = p.getString(0);
            int npos = Page.maxLength(s.length());
            int val = p.getInt(npos);
            records.add(new LogRecord(s, val));
        }
        return records;
    }

    private void createRecords(int start, int end) {
        for (int i = start; i <= end; i++) {
            byte[] rec = createLogRecord("record" + i, i + 100);
            int lsn = logMgr.append(rec);
            assertTrue(lsn >= 0);
        }
    }

    private byte[] createLogRecord(String s, int n) {
        int npos = Page.maxLength(s.length());
        byte[] b = new byte[npos + Integer.BYTES];
        Page p = new Page(b, fileMgr.arena());
        p.setString(0, s);
        p.setInt(npos, n);
        return p.toByteArray();
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

    private static class LogRecord {
        final String text;
        final int value;

        LogRecord(String text, int value) {
            this.text = text;
            this.value = value;
        }
    }
}
