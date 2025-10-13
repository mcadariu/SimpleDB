package com.simpledb;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.buffer.BufferMgr;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.file.FileMgr;
import com.simpledb.log.LogMgr;
import com.simpledb.metadata.MetadataMgr;
import com.simpledb.metadata.TableMgr;
import com.simpledb.transaction.Transaction;

import java.io.File;

public class SimpleDB {

    private FileMgr fileMgr;
    private LogMgr logMgr;
    private BufferMgr bufferMgr;
    private MetadataMgr metadataMgr;

    public SimpleDB(int blocksize, int buffsize) throws BufferAbortException, LockAbortException {
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "simpledb_test_" + System.currentTimeMillis());

        this.fileMgr = new FileMgr(tempDir, blocksize);
        this.logMgr = new LogMgr(fileMgr, "logtest");
        this.bufferMgr = new BufferMgr(fileMgr, logMgr, buffsize);

        Transaction tx = new Transaction(fileMgr, logMgr, bufferMgr);

        boolean isnew = fileMgr.isNew();
        if (isnew)
            System.out.println("creating new database");
        else {
            System.out.println("recovering existing database");
            tx.recover();
        }

        metadataMgr = new MetadataMgr(isnew, tx);
        tx.commit();
    }

    public MetadataMgr metadataMgr() {
        return metadataMgr;
    }

    public Transaction newTransaction() {
        return new Transaction(fileMgr, logMgr, bufferMgr);
    }
}
