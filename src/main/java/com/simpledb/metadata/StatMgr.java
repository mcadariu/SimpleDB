package com.simpledb.metadata;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.record.Layout;
import com.simpledb.scan.TableScan;
import com.simpledb.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class StatMgr {
    private TableMgr tableMgr;
    private Map<String, StatInfo> tablestats;
    private int numcalls;

    public StatMgr(TableMgr tableMgr, Transaction tx) throws BufferAbortException, LockAbortException {
        this.tableMgr = tableMgr;
        refreshStatistics(tx);
    }

    public synchronized StatInfo getStatInfo(String tblname, Layout layout, Transaction tx) throws BufferAbortException, LockAbortException {
        numcalls++;

        if (numcalls > 100)
            refreshStatistics(tx);

        StatInfo si = tablestats.get(tblname);
        if (si == null) {
            si = calcTableStats(tblname, layout, tx);
            tablestats.put(tblname, si);
        }
        return si;
    }

    private StatInfo calcTableStats(String tblname, Layout layout, Transaction tx) throws BufferAbortException, LockAbortException {
        int numrecs = 0;
        int numblocks = 0;

        TableScan tableScan = new TableScan(tx, tblname, layout);
        while (tableScan.next()) {
            numrecs++;
            numblocks = tableScan.getRid().blockNumber() + 1;
        }
        tableScan.close();
        return new StatInfo(numblocks, numrecs);
    }

    private void refreshStatistics(Transaction tx) throws BufferAbortException, LockAbortException {
        tablestats = new HashMap<>();
        numcalls = 0;
        Layout tcatLayout = tableMgr.getLayout("tblcat", tx);
        TableScan tcat = new TableScan(tx, "tblcat", tcatLayout);
        while (tcat.next()) {
            String tblname = tcat.getString("tblname");
            Layout layout = tableMgr.getLayout(tblname, tx);
            StatInfo statInfo = calcTableStats(tblname, layout, tx);
            tablestats.put(tblname, statInfo);
        }
        tcat.close();
    }
}
