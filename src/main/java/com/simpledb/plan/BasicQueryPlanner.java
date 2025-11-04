package com.simpledb.plan;

import com.simpledb.metadata.MetadataMgr;
import com.simpledb.parse.QueryData;
import com.simpledb.transaction.Transaction;

import java.util.ArrayList;
import java.util.List;

public class BasicQueryPlanner {
    private MetadataMgr metadataMgr;

    public BasicQueryPlanner(MetadataMgr metadataMgr) {
        this.metadataMgr = metadataMgr;
    }

    public Plan createPlan(QueryData queryData, Transaction tx) {
        List<Plan> plans = new ArrayList<>();

        for (String tblname : queryData.tables()) {
            plans.add(new TablePlan(tx, tblname, metadataMgr));
        }

        Plan p = plans.remove(0);
        for (Plan nextPlan : plans)
            p = new ProductPlan(p, nextPlan);

        p = new SelectPlan(p, queryData.pred());

        return new ProjectPlan(p, queryData.fields());
    }
}
