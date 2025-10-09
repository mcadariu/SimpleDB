package com.simpledb.concurrency;

import com.simpledb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class ConcurrencyMgr {
    private static LockTable lockTable = new LockTable();
    private Map<BlockId, String> locks = new HashMap<>();

    public void sLock(BlockId blockId) throws LockAbortException {
        if(locks.get(blockId) == null) {
            lockTable.sLock(blockId);
            locks.put(blockId, "S");
        }
    }

    public void xLock(BlockId blockId) throws LockAbortException {
        if(!hasXLock(blockId)) {
            sLock(blockId);
            lockTable.xLock(blockId);
            locks.put(blockId, "X");
        }
    }

    public void release() {
        for(BlockId blockId: locks.keySet()) {
            lockTable.unlock(blockId);
        }
        locks.clear();
    }

    private boolean hasXLock(BlockId blockId) {
        String locktype = locks.get(blockId);
        return locktype != null && locktype.equals("X");
    }
}
