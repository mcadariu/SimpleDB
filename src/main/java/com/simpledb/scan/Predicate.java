package com.simpledb.scan;

import com.simpledb.record.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Predicate {
    private List<Term> terms = new ArrayList<>();

    public Predicate() {
    }

    public Predicate(Term t) {
        terms.add(t);
    }

    public void conjoinWith(Predicate pred) {
        terms.addAll(pred.terms);
    }

    public boolean isSatisfied(Scan s) {
        for (Term t : terms)
            if (!t.isSatisfied(s))
                return false;
        return true;
    }

    public int reductionFactor(Plan p) {
        int factor = 1;
        for (Term t : terms) {
            factor *= t.reductionFactor(p);
        }
        return factor;
    }

    public Predicate joinSubPred(Schema sch1, Schema sch2) {
        Predicate result = new Predicate();
        Schema newSch = new Schema();
        newSch.addAll(sch1);
        newSch.addAll(sch2);

        for (Term t : terms) {
            if (!t.appliesTo(sch1) && !t.appliesTo(sch2) && t.appliesTo(newSch))
                result.terms.add(t);
        }

        if (result.terms.isEmpty())
            return null;
        else return result;
    }

    public Constant equatesWithConstant(String fldname) {
        for (Term t : terms) {
            Constant c = t.equatesWithConstant(fldname);
            if (c != null) {
                return c;
            }
        }
        return null;
    }

    public String equatesWithField(String fldname) {
        for (Term t : terms) {
            String s = t.equatesWithField(fldname);
            if (s != null)
                return s;
        }

        return null;
    }

    public String toString() {
        Iterator<Term> iter = terms.iterator();
        if (!iter.hasNext())
            return "";
        StringBuilder result = new StringBuilder(iter.next().toString());
        while (iter.hasNext())
            result.append(" and ").append(iter.next().toString());
        return result.toString();
    }
}
