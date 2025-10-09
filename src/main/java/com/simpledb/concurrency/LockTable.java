package com.simpledb.concurrency;

import com.simpledb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class LockTable {
    private static final long MAX_TIME = 10000;

    private Map<BlockId, Integer> locks = new HashMap<>();

    public synchronized void sLock(BlockId blockId) throws LockAbortException {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasXlock(blockId) && !waitingTooLong(timestamp))
                wait(MAX_TIME);
            if (hasXlock(blockId)) {
                throw new LockAbortException();
            }
            int val = getLockVal(blockId);
            locks.put(blockId, val + 1);
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    public synchronized void xLock(BlockId blockId) throws LockAbortException {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasOtherSlocks(blockId) && !waitingTooLong(timestamp))
                wait(MAX_TIME);
            if (hasOtherSlocks(blockId))
                throw new LockAbortException();
            locks.put(blockId, -1);
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    public synchronized void unlock(BlockId blockId) {
        int val = getLockVal(blockId);
        if (val > 1)
            locks.put(blockId, val - 1);
        else {
            locks.remove(blockId);
            notifyAll();
        }
    }

    private boolean hasXlock(BlockId blockId) {
        return getLockVal(blockId) < 0;
    }

    private boolean hasOtherSlocks(BlockId blockId) {
        return getLockVal(blockId) > 1;
    }

    private boolean waitingTooLong(long starttime) {
        return System.currentTimeMillis() - starttime > MAX_TIME;
    }

    private int getLockVal(BlockId blockId) {
        Integer ival = locks.get(blockId);
        return (ival == null) ? 0 : ival.intValue();
    }
}
