package com.simpledb.planning;

import com.simpledb.SimpleDB;
import com.simpledb.buffer.BufferAbortException;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.parsing.BadSyntaxException;
import com.simpledb.parsing.Parser;
import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.scan.Scan;
import com.simpledb.scan.TableScan;
import com.simpledb.transaction.Transaction;

public class PlannerTest {
    public static void main(String[] args) throws BufferAbortException, LockAbortException, BadSyntaxException {
        SimpleDB simpleDB = new SimpleDB(400, 8);
        Transaction tx = simpleDB.newTransaction();

        // Create and populate the student table
        createStudentData(simpleDB, tx);

        // Now run the query
        BasicQueryPlanner basicQueryPlanner = new BasicQueryPlanner(simpleDB.metadataMgr());
        String qry = "select sname, gradyear from student";
        Parser parser = new Parser(qry);
        Plan p = basicQueryPlanner.createPlan(parser.query(), tx);

        System.out.println("Query results:");
        Scan s = p.open();
        while (s.next()) {
            System.out.println(s.getString("sname") + " " + s.getInt("gradyear"));
        }
        s.close();

        tx.commit();
    }

    private static void createStudentData(SimpleDB simpleDB, Transaction tx) throws BufferAbortException, LockAbortException {
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
        System.out.println("Inserted 5 students into the database.\n");
    }
}
