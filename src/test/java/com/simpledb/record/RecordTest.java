package com.simpledb.record;

import com.simpledb.buffer.BufferAbortException;
import com.simpledb.buffer.BufferMgr;
import com.simpledb.concurrency.LockAbortException;
import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.log.LogMgr;
import com.simpledb.transaction.Transaction;

import java.io.File;

public class RecordTest {
    private static FileMgr fileMgr;
    private static LogMgr logMgr;
    private static BufferMgr bufferMgr;

    public static void main(String[] args) throws LockAbortException, BufferAbortException {
        Schema schema = new Schema();
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "simpledb_test_" + System.currentTimeMillis());
        fileMgr = new FileMgr(tempDir, 400);
        logMgr = new LogMgr(fileMgr, "logtest");
        bufferMgr = new BufferMgr(fileMgr, logMgr, 3);

        Transaction tx = new Transaction(fileMgr, logMgr, bufferMgr);

        schema.addIntField("A");
        schema.addStringField("B", 9);

        Layout layout = new Layout(schema);

        for (String fldname : layout.schema().fields()) {
            int offset = layout.offset(fldname);
            System.out.println(fldname + " has offset " + offset);
        }

        BlockId blockId = tx.append("testfile");

        tx.pin(blockId);
        RecordPage recordPage = new RecordPage(tx, blockId, layout);
        recordPage.format();

        System.out.println("Filling the page with random records...");
        int slot = recordPage.insertAfter(-1);

        while (slot >= 0) {
            int n = (int) Math.round(Math.random() * 50);
            recordPage.setInt(slot, "A", n);
            recordPage.setString(slot, "B", "rec" + n);
            System.out.println("Inserting into slot " + slot + ": (" + n + ", " + "rec" + n + "}");
            slot = recordPage.insertAfter(slot);
        }

        System.out.println("Deleted these records with A-values < 25.");
        int count = 0;
        slot = recordPage.nextAfter(-1);
        while (slot >= 0) {
            int a = recordPage.getInt(slot, "A");
            String b = recordPage.getString(slot, "B");
            if (a < 25) {
                count++;
                System.out.println("slot " + slot + ": {" + a + ", " + b + "}");
                recordPage.delete(slot);
            }
            slot = recordPage.nextAfter(slot);
        }

        System.out.println(count + " values under 25 were deleted.\n");
        System.out.println("Here are the remaining records:");

        slot = recordPage.nextAfter(-1);
        while (slot >= 0) {
            int a = recordPage.getInt(slot, "A");
            String b = recordPage.getString(slot, "B");
            System.out.println("slot " + slot + ": {" + a + ", " + b + "}");
            slot = recordPage.nextAfter(slot);
        }

        tx.unpin(blockId);
        tx.commit();
    }
}
