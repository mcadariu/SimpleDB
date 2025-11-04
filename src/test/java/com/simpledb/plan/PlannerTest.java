package com.simpledb.plan;

import com.simpledb.SimpleDB;
import com.simpledb.parse.Parser;
import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.scan.Scan;
import com.simpledb.scan.TableScan;
import com.simpledb.transaction.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PlannerTest {
    private SimpleDB simpleDB;

    @BeforeEach
    public void setUp() {
        simpleDB = new SimpleDB(400, 8);
    }

    @Test
    public void testBasicQueryPlanner() {
        Transaction tx = simpleDB.newTransaction();

        // Create and populate the student table
        createStudentData(simpleDB, tx);

        // Now run the query
        BasicQueryPlanner basicQueryPlanner = new BasicQueryPlanner(simpleDB.metadataMgr());
        String qry = "select sname, gradyear from student";
        Parser parser = new Parser(qry);
        Plan p = basicQueryPlanner.createPlan(parser.query(), tx);

        assertNotNull(p);

        List<StudentRecord> results = new ArrayList<>();
        Scan s = p.open();
        while (s.next()) {
            String sname = s.getString("sname");
            int gradyear = s.getInt("gradyear");
            results.add(new StudentRecord(sname, gradyear));
        }
        s.close();

        // Verify we got 5 students
        assertEquals(5, results.size());

        // Verify some expected data
        boolean foundAlice = false;
        boolean foundBob = false;
        for (StudentRecord record : results) {
            if ("Alice".equals(record.sname)) {
                foundAlice = true;
                assertEquals(2024, record.gradyear);
            } else if ("Bob".equals(record.sname)) {
                foundBob = true;
                assertEquals(2025, record.gradyear);
            }
        }
        assertTrue(foundAlice);
        assertTrue(foundBob);

        tx.commit();
    }

    private void createStudentData(SimpleDB simpleDB, Transaction tx) {
        // Create student table schema
        Schema studentSchema = new Schema();
        studentSchema.addIntField("sid");
        studentSchema.addStringField("sname", 20);
        studentSchema.addIntField("gradyear");
        studentSchema.addIntField("majorid");

        // Create the student table
        simpleDB.metadataMgr().createTable("student", studentSchema, tx);
        Layout studentLayout = simpleDB.metadataMgr().getLayout("student", tx);

        // Insert test data
        TableScan ts = new TableScan(tx, "student", studentLayout);

        // Insert student 1
        ts.insert();
        ts.setInt("sid", 1);
        ts.setString("sname", "Alice");
        ts.setInt("gradyear", 2024);

        // Insert student 2
        ts.insert();
        ts.setInt("sid", 2);
        ts.setString("sname", "Bob");
        ts.setInt("gradyear", 2025);
        ts.setInt("majorid", 20);

        // Insert student 3
        ts.insert();
        ts.setInt("sid", 3);
        ts.setString("sname", "Charlie");
        ts.setInt("gradyear", 2024);
        ts.setInt("majorid", 10);

        // Insert student 4
        ts.insert();
        ts.setInt("sid", 4);
        ts.setString("sname", "Diana");
        ts.setInt("gradyear", 2026);
        ts.setInt("majorid", 30);

        // Insert student 5
        ts.insert();
        ts.setInt("sid", 5);
        ts.setString("sname", "Eve");
        ts.setInt("gradyear", 2025);
        ts.setInt("majorid", 20);

        ts.close();
    }

    private static class StudentRecord {
        final String sname;
        final int gradyear;

        StudentRecord(String sname, int gradyear) {
            this.sname = sname;
            this.gradyear = gradyear;
        }
    }
}
