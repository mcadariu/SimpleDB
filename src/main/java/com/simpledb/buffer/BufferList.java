package com.simpledb.buffer;

import com.simpledb.file.BlockId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BufferList {
    private Map<BlockId, Buffer> buffers = new HashMap<>();
    private List<BlockId> pins = new ArrayList<>();
    private BufferMgr bufferMgr;

    public BufferList(BufferMgr bufferMgr) {
        this.bufferMgr = bufferMgr;
    }

    public Buffer getBuffer(BlockId blockId) {
        return buffers.get(blockId);
    }

    public void pin(BlockId blockId) throws BufferAbortException {
        Buffer buff = bufferMgr.pin(blockId);
        buffers.put(blockId, buff);
        pins.add(blockId);
    }

    public void unpin(BlockId blockId) {
        Buffer buffer = buffers.get(blockId);
        bufferMgr.unpin(buffer);
        pins.remove(blockId);
        if (!pins.contains(blockId))
            buffers.remove(blockId);
    }

    public void unpinAll() {
        for (BlockId blockId : pins) {
            Buffer buff = buffers.get(blockId);
            bufferMgr.unpin(buff);
        }
        buffers.clear();
        pins.clear();
    }
}
