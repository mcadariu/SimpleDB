package com.simpledb.index;

import com.simpledb.SimpleDB;
import com.simpledb.metadata.IndexInfo;
import com.simpledb.metadata.MetadataMgr;
import com.simpledb.plan.Plan;
import com.simpledb.plan.TablePlan;
import com.simpledb.record.Schema;
import com.simpledb.scan.Constant;
import com.simpledb.scan.RID;
import com.simpledb.scan.UpdateScan;
import com.simpledb.transaction.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class IndexUpdateTest {
    private SimpleDB simpleDB;

    @BeforeEach
    public void setUp() {
        simpleDB = new SimpleDB(400, 8);
    }

    @Test
    public void testIndexUpdate() {
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

        studentScan.beforeFirst();

        // Delete joe
        boolean foundJoe = false;
        while (studentScan.next()) {
            if (studentScan.getString("sname").equals("joe")) {
                foundJoe = true;
                assertEquals(10, studentScan.getInt("sid"));
                RID joeRid = studentScan.getRid();
                for (String fldname : indexes.keySet()) {
                    Constant dataVal = studentScan.getVal(fldname);
                    Index idx = indexes.get(fldname);
                    idx.delete(dataVal, joeRid);
                }
                studentScan.delete();
                break;
            }
        }
        assertTrue(foundJoe, "Should have found joe");

        // Verify remaining students
        studentScan.beforeFirst();
        List<String> remainingStudents = new ArrayList<>();
        while (studentScan.next()) {
            remainingStudents.add(studentScan.getString("sname"));
        }

        assertEquals(1, remainingStudents.size());
        assertTrue(remainingStudents.contains("sam"));
        assertFalse(remainingStudents.contains("joe"));

        studentScan.close();
        for (Index idx : indexes.values()) {
            idx.close();
        }
        tx.commit();
    }
}
