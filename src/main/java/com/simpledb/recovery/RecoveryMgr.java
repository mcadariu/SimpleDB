package com.simpledb.recovery;

import com.simpledb.buffer.Buffer;
import com.simpledb.buffer.BufferMgr;
import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.log.*;

import java.util.Iterator;

import static com.simpledb.log.LogRecord.START;

public class RecoveryMgr {

    private LogMgr logMgr;
    private BufferMgr bufferMgr;
    private Transaction transaction;
    private int txnum;
    private FileMgr fileMgr;

    public RecoveryMgr(Transaction tx, int txnum, LogMgr logMgr, BufferMgr bufferMgr, FileMgr fileMgr) {
        this.transaction = tx;
        this.txnum = txnum;
        this.logMgr = logMgr;
        this.bufferMgr = bufferMgr;
        this.fileMgr = fileMgr;
        StartRecord.writeToLog(logMgr, txnum);
    }

    public void commit() {
        bufferMgr.flushAll(txnum);
        int lsn = CommitRecord.writeToLog(logMgr, txnum);
        logMgr.flush(lsn);
    }

    public void rollback() {
        doRollback();
        bufferMgr.flushAll(txnum);
        int lsn = RollbackRecord.writeToLog(logMgr, txnum);
        logMgr.flush(lsn);
    }

    public void recover() {
        doRecover();
        bufferMgr.flushAll(txnum);
        int lsn = CheckpointRecord.writeToLog(logMgr);
        logMgr.flush(lsn);
    }

    public int setInt(Buffer buff, int offset, int newval) {
        int oldval = buff.contents().getInt(offset);
        BlockId blk = buff.block();
        return SetIntRecord.writeToLog(logMgr, txnum, blk, offset, oldval, fileMgr);
    }

    public int setString(Buffer buffer, int offset, String newval) {
        String oldval = buffer.contents().getString(offset);
        BlockId blk = buffer.block();
        return SetStringRecord.writeToLog(logMgr, txnum, blk, offset, oldval, fileMgr);
    }

    private void doRollback() {
        Iterator<byte[]> iter = logMgr.iterator();
        while(iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord rec = LogRecord.createLogRecord(bytes, fileMgr);
            if(rec.txNumber() == txnum) {
                if(rec.op() == START) {
                    return;
                }
                rec.undo(transaction);
            }
        }
    }
}
