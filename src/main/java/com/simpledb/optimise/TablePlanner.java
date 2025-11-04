package com.simpledb.optimise;

import com.simpledb.metadata.IndexInfo;
import com.simpledb.metadata.MetadataMgr;
import com.simpledb.plan.*;
import com.simpledb.record.Schema;
import com.simpledb.scan.Constant;
import com.simpledb.scan.Predicate;
import com.simpledb.transaction.Transaction;

import java.util.Map;

public class TablePlanner {
    private TablePlan myplan;
    private Predicate mypred;
    private Schema myschema;
    private Map<String, IndexInfo> indexes;
    private Transaction tx;

    public TablePlanner(String tblname, Predicate pred, Transaction tx, MetadataMgr metadataMgr) {
        this.mypred = pred;
        this.tx = tx;
        myplan = new TablePlan(tx, tblname, metadataMgr);
        myschema = myplan.schema();
        indexes = metadataMgr.getIndexInfo(tblname, tx);
    }

    public Plan makeSelectPlan() {
        Plan p = makeIndexSelect();
        if (p == null)
            p = myplan;
        return addSelectPred(p);
    }

    public Plan makeJoinPlan(Plan current) {
        Schema currsch = current.schema();
        Predicate joinPred = mypred.joinSubPred(myschema, currsch);

        if (joinPred == null)
            return null;
        Plan p = makeIndexJoin(current, currsch);

        if (p == null)
            p = makeProductJoin(current, currsch);
        return p;
    }

    public Plan makeProductPlan(Plan current) {
        Plan p = addSelectPred(myplan);
        return new ProductPlan(current, p);
    }

    private Plan makeProductJoin(Plan current, Schema currsch) {
        Plan p = makeProductPlan(current);
        return addJoinPred(p, currsch);
    }

    private Plan makeIndexJoin(Plan current, Schema currsch) {
        for (String fldname : indexes.keySet()) {
            String outerfield = mypred.equatesWithField(fldname);
            if (outerfield != null && currsch.hasField(outerfield)) {
                IndexInfo ii = indexes.get(fldname);
                Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
                p = addSelectPred(p);
                return addJoinPred(p, currsch);
            }
        }
        return null;
    }

    private Plan addJoinPred(Plan p, Schema currsch) {
        Predicate joinPred = mypred.joinSubPred(currsch, myschema);
        if (joinPred != null)
            return new SelectPlan(p, joinPred);
        else
            return p;
    }

    private Plan addSelectPred(Plan p) {
        Predicate selectpred = mypred.selectSubPred(myschema);
        if (selectpred != null)
            return new SelectPlan(p, selectpred);
        else
            return p;
    }

    private Plan makeIndexSelect() {
        for (String fldname : indexes.keySet()) {
            Constant val = mypred.equatesWithConstant(fldname);
            if (val != null) {
                IndexInfo ii = indexes.get(fldname);
                return new IndexSelectPlan(myplan, ii, val);
            }
        }

        return null;
    }
}
