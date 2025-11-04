package com.simpledb.transaction;

import com.simpledb.buffer.Buffer;
import com.simpledb.buffer.BufferList;
import com.simpledb.buffer.BufferMgr;
import com.simpledb.concurrency.ConcurrencyMgr;
import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.file.Page;
import com.simpledb.log.LogMgr;
import com.simpledb.recover.RecoveryMgr;

public class Transaction {
    private static int nextTxNum = 0;
    private static final int END_OF_FILE = -1;
    private static RecoveryMgr recoveryMgr;
    private ConcurrencyMgr concurrencyMgr;
    private BufferMgr bufferMgr;
    private FileMgr fileMgr;
    private int txnum;
    private BufferList myBuffers;

    public Transaction(FileMgr fileMgr, LogMgr logMgr, BufferMgr bufferMgr) {
        this.fileMgr = fileMgr;
        this.bufferMgr = bufferMgr;
        txnum = nextTxNumber();
        recoveryMgr = new RecoveryMgr(this, txnum, logMgr, bufferMgr, fileMgr);

        concurrencyMgr = new ConcurrencyMgr();
        myBuffers = new BufferList(bufferMgr);
    }

    public void commit() {
        recoveryMgr.commit();
        concurrencyMgr.release();
        myBuffers.unpinAll();
        System.out.println("transaction " + txnum + " committed");
    }

    public void rollback() {
        recoveryMgr.rollback();
        concurrencyMgr.release();
        myBuffers.unpinAll();
        System.out.println("transaction " + txnum + " rollbacked");
    }

    public void recover() {
        bufferMgr.flushAll(txnum);
        recoveryMgr.recover();
    }

    public void pin(BlockId blockId) {
        myBuffers.pin(blockId);
    }

    public void unpin(BlockId blockId) {
        myBuffers.unpin(blockId);
    }

    public int getInt(BlockId blockId, int offset) {
        concurrencyMgr.sLock(blockId);
        Buffer buff = myBuffers.getBuffer(blockId);
        return buff.contents().getInt(offset);
    }

    public String getString(BlockId blockId, int offset) {
        concurrencyMgr.sLock(blockId);
        Buffer buff = myBuffers.getBuffer(blockId);
        return buff.contents().getString(offset);
    }

    public void setInt(BlockId blockId, int offset, int val, boolean okToLong) {
        concurrencyMgr.xLock(blockId);
        Buffer buff = myBuffers.getBuffer(blockId);
        int lsn = -1;
        if (okToLong) {
            lsn = recoveryMgr.setInt(buff, offset, val);
        }

        Page p = buff.contents();
        p.setInt(offset, val);
        buff.setModified(txnum, lsn);
    }

    public void setString(BlockId blockId, int offset, String val, boolean okToLog) {
        concurrencyMgr.xLock(blockId);

        Buffer buff = myBuffers.getBuffer(blockId);
        int lsn = -1;
        if (okToLog) {
            lsn = recoveryMgr.setString(buff, offset, val);
        }

        Page p = buff.contents();
        p.setString(offset, val);
        buff.setModified(txnum, lsn);
    }

    public int size(String filename) {
        BlockId dummyblk = new BlockId(filename, END_OF_FILE);
        concurrencyMgr.sLock(dummyblk);
        return fileMgr.length(filename);
    }

    public BlockId append(String filename) {
        BlockId dummyblk = new BlockId(filename, END_OF_FILE);
        concurrencyMgr.xLock(dummyblk);
        return fileMgr.append(filename);
    }

    public int blockSize() {
        return fileMgr.blockSize();
    }

    public int availableBuffs() {
        return bufferMgr.available();
    }

    private static synchronized int nextTxNumber() {
        nextTxNum++;
        System.out.println("new transaction: " + nextTxNum);
        return nextTxNum;
    }
}
