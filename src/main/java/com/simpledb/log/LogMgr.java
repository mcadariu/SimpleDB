package com.simpledb.log;

import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.file.Page;

import java.util.Iterator;

public class LogMgr {
    private FileMgr fileMgr;
    private String logFile;
    private Page logpage;
    private BlockId currentblk;
    private int latestLSN = 0;
    private int lastSavedLSN = 0;

    public LogMgr(FileMgr fileMgr, String logFile) {
        this.fileMgr = fileMgr;
        this.logFile = logFile;

        byte[] b = new byte[fileMgr.blockSize()];
        logpage = new Page(b, fileMgr.arena());
        int logsize = fileMgr.length(logFile);

        if (logsize == 0) {
            currentblk = appendNewBlock();
        } else {
            currentblk = new BlockId(logFile, logsize - 1);
            fileMgr.read(currentblk, logpage);
        }
    }

    public void flush(int lsn) {
        if (lsn >= lastSavedLSN) flush();
    }

    public Iterator<byte[]> iterator() {
        flush();
        return new LogIterator(fileMgr, currentblk);
    }

    public synchronized int append(byte[] logrec) {
        int boundary = logpage.getInt(0);
        int recsize = logrec.length;

        int bytesneeded = recsize + Integer.BYTES;

        //doesn't fit
        if (boundary - bytesneeded < Integer.BYTES) {
            flush();
            currentblk = appendNewBlock();
            boundary = logpage.getInt(0);
        }

        int recpos = boundary - bytesneeded;
        logpage.setBytes(recpos, logrec);
        logpage.setInt(0, recpos);
        latestLSN += 1;
        return latestLSN;
    }

    private BlockId appendNewBlock() {
        BlockId blk = fileMgr.append(logFile);
        logpage.clear();
        logpage.setInt(0, fileMgr.blockSize());
        fileMgr.write(blk, logpage);
        return blk;
    }

    private void flush() {
        fileMgr.write(currentblk, logpage);
        lastSavedLSN = latestLSN;
    }
}
