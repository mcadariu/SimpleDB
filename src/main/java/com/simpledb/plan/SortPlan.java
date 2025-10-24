package com.simpledb.plan;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.file.TempTable;
import com.simpledb.record.Schema;
import com.simpledb.scan.Scan;
import com.simpledb.scan.SortScan;
import com.simpledb.scan.UpdateScan;
import com.simpledb.transaction.Transaction;

import java.util.ArrayList;
import java.util.List;

public class SortPlan implements Plan {

    private Plan p;
    private Transaction tx;
    private Schema sch;
    private RecordComparator comp;

    public SortPlan(Plan p, List<String> sortFields, Transaction tx) {
        this.p = p;
        this.tx = tx;
        sch = p.schema();
        comp = new RecordComparator(sortFields);
    }

    @Override
    public Scan open() throws BufferAbortException, LockAbortException {
        Scan src = p.open();
        List<TempTable> runs = splitIntoRuns(src);
        src.close();

        while (runs.size() > 2)
            runs = doAMergeIteration(runs);
        return new SortScan(runs, comp);
    }

    private List<TempTable> doAMergeIteration(List<TempTable> runs) throws BufferAbortException, LockAbortException {
        List<TempTable> result = new ArrayList<>();
        while (runs.size() > 1) {
            TempTable p1 = runs.remove(0);
            TempTable p2 = runs.remove(0);
            result.add(mergeTwoRuns(p1, p2));
        }
        if (runs.size() == 1)
            result.add(runs.get(0));
        return result;
    }

    private TempTable mergeTwoRuns(TempTable p1, TempTable p2) throws BufferAbortException, LockAbortException {
        Scan src1 = p1.open();
        Scan src2 = p2.open();
        TempTable result = new TempTable(tx, sch);
        UpdateScan dest = result.open();

        boolean hasmore1 = src1.next();
        boolean hasmore2 = src2.next();

        while (hasmore1 && hasmore2) {
            if (comp.compare(src1, src2) < 0) {
                hasmore1 = copy(src1, dest);
            } else {
                hasmore2 = copy(src2, dest);
            }
        }

        if (hasmore1) {
            while (hasmore1)
                hasmore1 = copy(src1, dest);

        } else {
            while (hasmore2)
                hasmore2 = copy(src2, dest);
        }

        src1.close();
        src2.close();
        dest.close();

        return result;
    }

    private List<TempTable> splitIntoRuns(Scan src) throws BufferAbortException, LockAbortException {
        List<TempTable> temps = new ArrayList<>();
        src.beforeFirst();
        if (!src.next())
            return temps;

        TempTable currentTemp = new TempTable(tx, sch);
        temps.add(currentTemp);
        UpdateScan currentScan = currentTemp.open();
        while (copy(src, currentScan)) {
            if (comp.compare(src, currentScan) < 0) {
                currentScan.close();
                currentTemp = new TempTable(tx, sch);
                temps.add(currentTemp);
                currentScan = (UpdateScan) currentTemp.open();
            }
        }
        currentScan.close();
        return temps;
    }

    private boolean copy(Scan src, UpdateScan dest) throws BufferAbortException, LockAbortException {
        dest.insert();
        for(String fldname: sch.fields())
            dest.setVal(fldname, src.getVal(fldname));
        return src.next();
    }

    @Override
    public int blocksAccessed() {
        Plan mp = new MaterializePlan(tx, p);
        return mp.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return p.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return sch;
    }
}
