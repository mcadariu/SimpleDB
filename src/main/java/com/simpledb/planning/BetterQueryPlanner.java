package com.simpledb.planning;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.metadata.MetadataMgr;
import com.simpledb.parsing.QueryData;
import com.simpledb.transaction.Transaction;

import java.util.ArrayList;
import java.util.List;

public class BetterQueryPlanner {
    private MetadataMgr metadataMgr;

    public BetterQueryPlanner(MetadataMgr metadataMgr) {
        this.metadataMgr = metadataMgr;
    }

    public Plan createPlan(QueryData queryData, Transaction tx) throws BufferAbortException, LockAbortException {
        List<Plan> plans = new ArrayList<>();

        for (String tblname : queryData.tables()) {
            plans.add(new TablePlan(tx, tblname, metadataMgr));
        }

        Plan p = plans.remove(0);
        for (Plan nextPlan : plans) {
            Plan p1 = new ProductPlan(nextPlan, p);
            Plan p2 = new ProductPlan(p, nextPlan);

            p = (p1.blocksAccessed() < p2.blocksAccessed()) ? p1 : p2;
        }

        p = new SelectPlan(p, queryData.pred());

        return new ProjectPlan(p, queryData.fields());
    }
}
