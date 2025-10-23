package com.simpledb.index;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.record.Layout;
import com.simpledb.scan.Constant;
import com.simpledb.scan.RID;
import com.simpledb.scan.TableScan;
import com.simpledb.transaction.Transaction;

public class HashIndex implements Index {
    public static int NUM_BUCKERS = 100;
    private Transaction tx;
    private String idxname;
    private Layout layout;
    private Constant searchkey = null;
    private TableScan tableScan = null;

    public HashIndex(Transaction tx, String idxname, Layout layout) {
        this.tx = tx;
        this.idxname = idxname;
        this.layout = layout;
    }

    @Override
    public void beforeFirst(Constant searchKey) throws BufferAbortException, LockAbortException {
        close();
        this.searchkey = searchKey;
        int bucket = searchKey.hashCode() % NUM_BUCKERS;
        String tblname = idxname + bucket;
        tableScan = new TableScan(tx, tblname, layout);
    }

    @Override
    public boolean next() throws BufferAbortException, LockAbortException {
        while (tableScan.next()) {
            if (tableScan.getVal("dataval").equals(searchkey))
                return true;
        }
        return false;
    }

    @Override
    public RID getDataRid() throws LockAbortException {
        int blknum = tableScan.getInt("block");
        int id = tableScan.getInt("id");
        return new RID(blknum, id);
    }

    @Override
    public void insert(Constant dataVal, RID dataRid) throws BufferAbortException, LockAbortException {
        beforeFirst(dataVal);
        tableScan.insert();
        tableScan.setInt("block", dataRid.blockNumber());
        tableScan.setInt("id", dataRid.slot());
        tableScan.setVal("dataval", dataVal);
    }

    @Override
    public void delete(Constant dataval, RID datarid) throws BufferAbortException, LockAbortException {
        beforeFirst(dataval);
        while (next()) {
            if (getDataRid().equals(datarid)) {
                tableScan.delete();
                return;
            }
        }
    }

    @Override
    public void close() {
        if (tableScan != null)
            tableScan.close();
    }

    public static int searchCost(int numblocks, int rpb) {
        return numblocks / HashIndex.NUM_BUCKERS;
    }
}
