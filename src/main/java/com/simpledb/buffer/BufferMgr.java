package com.simpledb.buffer;

import com.simpledb.file.BlockId;
import com.simpledb.file.FileMgr;
import com.simpledb.log.LogMgr;

public class BufferMgr {
    private Buffer[] bufferpool;
    private int numAvailable;
    private static final long MAX_TIME = 10000;

    public BufferMgr(FileMgr fileMgr, LogMgr logMgr, int numbuffs) {
        bufferpool = new Buffer[numbuffs];
        numAvailable = numbuffs;
        for (int i = 0; i < numbuffs; i++) {
            bufferpool[i] = new Buffer(fileMgr, logMgr);
        }
    }

    public synchronized int available() {
        return numAvailable;
    }

    public synchronized void flushAll(int txnum) {
        for (Buffer buffer : bufferpool) {
            if (buffer.modifyingTx() == txnum)
                buffer.flush();
        }
    }

    public synchronized void unpin(Buffer buffer) {
        buffer.unpin();
        if (!buffer.isPinned()) {
            numAvailable++;
            notifyAll();
        }
    }

    public synchronized Buffer pin(BlockId blockId) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buffer = tryToPin(blockId);
            while (buffer == null && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
                buffer = tryToPin(blockId);
            }

            if (buffer == null) {
                throw new BufferAbortException();
            }
            return buffer;
        } catch (InterruptedException e) {
            throw new BufferAbortException();
        }
    }

    private boolean waitingTooLong(long starttime) {
        return System.currentTimeMillis() - starttime > MAX_TIME;
    }

    private Buffer tryToPin(BlockId blockId) {
        Buffer buffer = findExistingBuffer(blockId);
        if (buffer == null) {
            buffer = chooseUnpinnedBuffer();
            if (buffer == null) {
                return null;
            }
            buffer.assignToBlock(blockId);
        }

        if (!buffer.isPinned()) {
            numAvailable--;
        }
        buffer.pin();
        return buffer;
    }

    private Buffer findExistingBuffer(BlockId blockId) {
        for (Buffer buffer : bufferpool) {
            BlockId b = buffer.block();
            if (b != null && b.equals(blockId)) {
                return buffer;
            }
        }
        return null;
    }

    private Buffer chooseUnpinnedBuffer() {
        for (Buffer buffer : bufferpool) {
            if (!buffer.isPinned())
                return buffer;
        }
        return null;
    }
}
