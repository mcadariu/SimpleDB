package com.simpledb.metadata;

import com.simpledb.SimpleDB;
import com.simpledb.record.Layout;
import com.simpledb.record.Schema;
import com.simpledb.scan.TableScan;
import com.simpledb.transaction.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class MetadataMgrTest {
    private SimpleDB simpleDB;

    @BeforeEach
    public void setUp() {
        simpleDB = new SimpleDB(400, 8);
    }

    @Test
    public void testCreateTableAndGetLayout() {
        Transaction tx = simpleDB.newTransaction();
        MetadataMgr metadataMgr = new MetadataMgr(true, tx);

        Schema schema = new Schema();
        schema.addIntField("A");
        schema.addStringField("B", 9);

        metadataMgr.createTable("MyTable", schema, tx);
        Layout layout = metadataMgr.getLayout("MyTable", tx);

        assertNotNull(layout);
        assertTrue(layout.slotsize() > 0);

        Schema schema2 = layout.schema();
        assertNotNull(schema2);

        // Verify field types
        int fieldCount = 0;
        for (String fldname : schema2.fields()) {
            fieldCount++;
            if ("A".equals(fldname)) {
                assertEquals(Types.INTEGER, schema2.type(fldname));
            } else if ("B".equals(fldname)) {
                assertEquals(Types.VARCHAR, schema2.type(fldname));
                assertEquals(9, schema2.length(fldname));
            }
        }
        assertEquals(2, fieldCount);
    }

    @Test
    public void testStatisticsInfo() {
        Transaction tx = simpleDB.newTransaction();
        MetadataMgr metadataMgr = new MetadataMgr(true, tx);

        Schema schema = new Schema();
        schema.addIntField("A");
        schema.addStringField("B", 9);

        metadataMgr.createTable("MyTable", schema, tx);
        Layout layout = metadataMgr.getLayout("MyTable", tx);

        // Insert test data
        TableScan ts = new TableScan(tx, "MyTable", layout);
        for (int i = 0; i < 50; i++) {
            ts.insert();
            int n = (int) Math.round(Math.random() * 50);
            ts.setInt("A", n);
            ts.setString("B", "rec" + n);
        }
        ts.close();

        StatInfo si = metadataMgr.getStatInfo("MyTable", layout, tx);
        assertNotNull(si);
        assertTrue(si.blocksAccessed() > 0);
        assertEquals(50, si.recordsOutput());
        assertTrue(si.distinctValues("A") > 0);
        assertTrue(si.distinctValues("B") > 0);
    }

    @Test
    public void testIndexInfo() {
        Transaction tx = simpleDB.newTransaction();
        MetadataMgr metadataMgr = new MetadataMgr(true, tx);

        Schema schema = new Schema();
        schema.addIntField("A");
        schema.addStringField("B", 9);

        metadataMgr.createTable("MyTable", schema, tx);
        Layout layout = metadataMgr.getLayout("MyTable", tx);

        // Insert test data
        TableScan ts = new TableScan(tx, "MyTable", layout);
        for (int i = 0; i < 50; i++) {
            ts.insert();
            int n = (int) Math.round(Math.random() * 50);
            ts.setInt("A", n);
            ts.setString("B", "rec" + n);
        }
        ts.close();

        // Create indexes
        metadataMgr.createIndex("indexA", "MyTable", "A", tx);
        metadataMgr.createIndex("indexB", "MyTable", "B", tx);

        Map<String, IndexInfo> idxmap = metadataMgr.getIndexInfo("MyTable", tx);
        assertNotNull(idxmap);
        assertEquals(2, idxmap.size());

        // Verify indexA
        IndexInfo iiA = idxmap.get("A");
        assertNotNull(iiA);
        assertTrue(iiA.blocksAccessed() >= 0);
        assertTrue(iiA.recordsOutput() >= 0);
        assertTrue(iiA.distinctValues("A") >= 0);

        // Verify indexB
        IndexInfo iiB = idxmap.get("B");
        assertNotNull(iiB);
        assertTrue(iiB.blocksAccessed() >= 0);
        assertTrue(iiB.recordsOutput() >= 0);
        assertTrue(iiB.distinctValues("B") >= 0);
    }
}
