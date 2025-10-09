package com.simpledb.log;

import com.simpledb.file.FileMgr;
import com.simpledb.file.Page;

public interface LogRecord {
    static final int CHECKPOINT = 0, START = 1, COMMIT = 2, ROLLBACK = 3, SETINT = 4, SETSTRING = 5;

    int op();

    int txNumber();

    void undo(Transaction tx);

    static LogRecord createLogRecord(byte[] bytes, FileMgr fileMgr) {
        Page p = new Page(bytes, fileMgr.arena());

        switch (p.getInt(0)) {
            case CHECKPOINT -> {
                return new CheckpointRecord(p);
            }
            case START -> {
                return new StartRecord(p);
            }
            case COMMIT -> {
                return new CommitRecord(p);
            }
            case ROLLBACK -> {
                return new RollbackRecord(p);
            }
            case SETINT -> {
                return new SetIntRecord(p);
            }
            case SETSTRING -> {
                return new SetStringRecord(p);
            }
            default -> {
                return null;
            }
        }
    }
}
