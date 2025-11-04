package com.simpledb.optimise;

import com.simpledb.metadata.MetadataMgr;
import com.simpledb.parse.QueryData;
import com.simpledb.plan.Plan;
import com.simpledb.plan.ProjectPlan;
import com.simpledb.transaction.Transaction;

import java.util.ArrayList;
import java.util.Collection;

public class HeuristicQueryPlanner {
    private Collection<TablePlanner> tablePlanners = new ArrayList<>();
    private MetadataMgr metadataMgr;

    public HeuristicQueryPlanner(MetadataMgr metadataMgr) {
        this.metadataMgr = metadataMgr;
    }

    public Plan createPlan(QueryData queryData, Transaction tx) {

        for (String tblname : queryData.tables()) {
            TablePlanner tp = new TablePlanner(tblname, queryData.pred(), tx, metadataMgr);
            tablePlanners.add(tp);
        }

        Plan currentPlan = getLowestSelectPlan();

        while (!tablePlanners.isEmpty()) {
            Plan p = getLowestJoinPlan(currentPlan);

            if (p != null)
                currentPlan = p;
            else
                currentPlan = getLowestProductPlan(currentPlan);
        }

        return new ProjectPlan(currentPlan, queryData.fields());
    }

    private Plan getLowestSelectPlan() {
        TablePlanner besttp = null;

        Plan bestPlan = null;
        for (TablePlanner tp : tablePlanners) {
            Plan plan = tp.makeSelectPlan();
            if (bestPlan == null || plan.recordsOutput() < bestPlan.recordsOutput()) {
                besttp = tp;
                bestPlan = plan;
            }
        }
        tablePlanners.remove(besttp);
        return bestPlan;
    }

    private Plan getLowestJoinPlan(Plan current) {
        TablePlanner besttp = null;
        Plan bestPlan = null;

        for (TablePlanner tp : tablePlanners) {
            Plan plan = tp.makeJoinPlan(current);
            if (plan != null && (bestPlan == null || plan.recordsOutput() < bestPlan.recordsOutput())) {
                besttp = tp;
                bestPlan = plan;
            }
        }

        if (bestPlan != null)
            tablePlanners.remove(besttp);
        return bestPlan;
    }

    private Plan getLowestProductPlan(Plan current) {
        TablePlanner besttp = null;
        Plan bestPlan = null;

        for (TablePlanner tp : tablePlanners) {
            Plan plan = tp.makeProductPlan(current);
            if (bestPlan == null || plan.recordsOutput() < bestPlan.recordsOutput()) {
                besttp = tp;
                bestPlan = plan;
            }
        }

        tablePlanners.remove(besttp);
        return bestPlan;
    }
}
