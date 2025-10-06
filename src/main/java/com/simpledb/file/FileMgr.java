package com.simpledb.file;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public class FileMgr {
    private File dbDirectory;
    private int blocksize;
    private boolean isNew;
    private Map<String, MemorySegment> mappedFiles = new HashMap<>();
    private final Arena arena;
    private static final int INITIAL_BLOCKS = 100;
    private static final int GROWTH_FACTOR = 2;

    public FileMgr(File dbDirectory, int blocksize) {
        this.dbDirectory = dbDirectory;
        this.blocksize = blocksize;
        this.arena = Arena.ofShared();

        isNew = !dbDirectory.exists();

        if (isNew)
            dbDirectory.mkdirs();

        String[] files = dbDirectory.list();
        if (files != null) {
            for (String filename : files) {
                if (filename.startsWith("temp"))
                    new File(dbDirectory, filename).delete();
            }
        }
    }

    public synchronized void read(BlockId blk, Page p) {
        try {
            MemorySegment mapped = getMappedFile(blk.fileName());
            long offset = blk.number() * (long) blocksize;
            MemorySegment blockSegment = mapped.asSlice(offset, blocksize);
            MemorySegment.copy(blockSegment, 0, p.contents(), 0, blocksize);
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + blk);
        }
    }

    public synchronized void write(BlockId blk, Page p) {
        try {
            MemorySegment mapped = getMappedFile(blk.fileName());
            long offset = blk.number() * (long) blocksize;
            long requiredLength = (blk.number() + 1) * (long) blocksize;

            if (mapped.byteSize() < requiredLength) {
                long currentBlocks = mapped.byteSize() / blocksize;
                long newBlocks = Math.max(currentBlocks * GROWTH_FACTOR, blk.number() + 1);
                long newSize = newBlocks * blocksize;
                remapFile(blk.fileName(), newSize);
                mapped = getMappedFile(blk.fileName());
            }

            MemorySegment blockSegment = mapped.asSlice(offset, blocksize);
            MemorySegment.copy(p.contents(), 0, blockSegment, 0, blocksize);
        } catch (IOException e) {
            throw new RuntimeException("cannot write block " + blk);
        }
    }

    public synchronized BlockId append(String filename) {
        int newblknum = length(filename);
        BlockId blk = new BlockId(filename, newblknum);

        Page emptyPage = new Page(blocksize, arena);
        write(blk, emptyPage);

        return blk;
    }

    public int length(String filename) {
        try {
            MemorySegment mapped = getMappedFile(filename);
            return (int) (mapped.byteSize() / blocksize);
        } catch (IOException e) {
            throw new RuntimeException("Cannot access " + filename);
        }
    }

    public boolean isNew() {
        return isNew;
    }

    public int blockSize() {
        return blocksize;
    }

    private MemorySegment getMappedFile(String filename) throws IOException {
        MemorySegment mapped = mappedFiles.get(filename);
        if (mapped == null) {
            File dbTable = new File(dbDirectory, filename);
            try (FileChannel channel = FileChannel.open(dbTable.toPath(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE)) {

                long fileSize = channel.size();
                long mapSize = fileSize > 0 ? fileSize : INITIAL_BLOCKS * (long) blocksize;

                if (fileSize < mapSize) {
                    channel.truncate(mapSize);
                }

                mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, mapSize, arena);
                mappedFiles.put(filename, mapped);
            }
        }
        return mapped;
    }

    private void remapFile(String filename, long newSize) throws IOException {
        mappedFiles.remove(filename);
        File dbTable = new File(dbDirectory, filename);

        try (FileChannel channel = FileChannel.open(dbTable.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {

            MemorySegment mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, newSize, arena);
            mappedFiles.put(filename, mapped);
        }
    }

    public Arena arena() {
        return arena;
    }
}
