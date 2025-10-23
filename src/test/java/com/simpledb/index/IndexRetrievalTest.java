package com.simpledb.index;

import com.simpledb.SimpleDB;
import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.metadata.IndexInfo;
import com.simpledb.metadata.MetadataMgr;
import com.simpledb.plan.Plan;
import com.simpledb.plan.TablePlan;
import com.simpledb.record.Schema;
import com.simpledb.scan.Constant;
import com.simpledb.scan.IntConstant;
import com.simpledb.scan.RID;
import com.simpledb.scan.UpdateScan;
import com.simpledb.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class IndexRetrievalTest {
    public static void main(String[] args) throws BufferAbortException, LockAbortException {
        SimpleDB simpleDB = new SimpleDB(400, 8);
        Transaction tx = simpleDB.newTransaction();

        MetadataMgr metadataMgr = simpleDB.metadataMgr();

        // Create student table schema
        Schema studentSchema = new Schema();
        studentSchema.addIntField("sid");
        studentSchema.addStringField("sname", 20);
        studentSchema.addIntField("gradyear");
        studentSchema.addIntField("majorid");

        // Create the student table
        metadataMgr.createTable("student", studentSchema, tx);

        // Create indexes on student fields
        metadataMgr.createIndex("sidindex", "student", "sid", tx);
        metadataMgr.createIndex("majoridindex", "student", "majorid", tx);

        Plan studentPlan = new TablePlan(tx, "student", metadataMgr);

        UpdateScan studentScan = (UpdateScan) studentPlan.open();

        Map<String, Index> indexes = new HashMap<>();
        Map<String, IndexInfo> idxinfo = metadataMgr.getIndexInfo("student", tx);

        for (String fldname : idxinfo.keySet()) {
            Index idx = idxinfo.get(fldname).open();
            indexes.put(fldname, idx);
        }

        // Insert joe
        studentScan.insert();
        studentScan.setInt("sid", 10);
        studentScan.setString("sname", "joe");
        studentScan.setInt("gradyear", 2020);
        studentScan.setInt("majorid", 20);

        RID joeDatarid = studentScan.getRid();
        for (String fldname : indexes.keySet()) {
            Constant dataVal = studentScan.getVal(fldname);
            Index idx = indexes.get(fldname);
            idx.insert(dataVal, joeDatarid);
        }

        // Insert sam
        studentScan.insert();
        studentScan.setInt("sid", 11);
        studentScan.setString("sname", "sam");
        studentScan.setInt("gradyear", 30);
        studentScan.setInt("majorid", 30);

        RID samDatarid = studentScan.getRid();
        for (String fldname : indexes.keySet()) {
            Constant dataVal = studentScan.getVal(fldname);
            Index idx = indexes.get(fldname);
            idx.insert(dataVal, samDatarid);
        }

        Index idx = indexes.get("majorid");

        idx.beforeFirst(new IntConstant(30));
        while (idx.next()) {
            RID datarid = idx.getDataRid();
            studentScan.moveToRid(datarid);
            System.out.println(studentScan.getString("sname"));
        }

        studentScan.close();
        idx.close();
        tx.commit();
    }
}
