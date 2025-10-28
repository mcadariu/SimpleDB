package com.simpledb.recover;

import com.simpledb.buffer.Buffer;
import com.simpledb.buffer.BufferAbortException;
import com.simpledb.buffer.BufferMgr;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.log.*;
import com.simpledb.transaction.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static com.simpledb.log.LogRecord.*;

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
        StartRecord.writeToLog(logMgr, txnum, fileMgr);
    }

    public void commit() {
        bufferMgr.flushAll(txnum);
        int lsn = CommitRecord.writeToLog(logMgr, txnum, fileMgr);
        logMgr.flush(lsn);
    }

    public void rollback() throws BufferAbortException, LockAbortException {
        doRollback();
        bufferMgr.flushAll(txnum);
        int lsn = RollbackRecord.writeToLog(logMgr, txnum, fileMgr);
        logMgr.flush(lsn);
    }

    public void recover() throws BufferAbortException, LockAbortException {
        doRecover();
        bufferMgr.flushAll(txnum);
        int lsn = CheckpointRecord.writeToLog(logMgr, fileMgr);
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

    private void doRollback() throws BufferAbortException, LockAbortException {
        Iterator<byte[]> iter = logMgr.iterator();
        while (iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord rec = LogRecord.createLogRecord(bytes, fileMgr);
            if (rec.txNumber() == txnum) {
                if (rec.op() == START) {
                    return;
                }
                rec.undo(transaction);
            }
        }
    }

    private void doRecover() throws BufferAbortException, LockAbortException {
        Collection<Integer> finishedTxs = new ArrayList<>();
        Iterator<byte[]> iter = logMgr.iterator();
        while (iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord rec = LogRecord.createLogRecord(bytes, fileMgr);
            if (rec.op() == CHECKPOINT) {
                return;
            }
            if (rec.op() == COMMIT || rec.op() == ROLLBACK) {
                finishedTxs.add(rec.txNumber());
            } else if (!finishedTxs.contains(rec.txNumber()))
                rec.undo(transaction);
        }
    }
}
