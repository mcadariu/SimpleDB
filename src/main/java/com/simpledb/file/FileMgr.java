package com.simpledb.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.util.HashMap;
import java.util.Map;

public class FileMgr {
    private final File dbDirectory;
    private final int blocksize;
    private final boolean isNew;
    private final Map<String, RandomAccessFile> openFiles = new HashMap<>();
    private final Arena arena;

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
            RandomAccessFile file = getFile(blk.fileName());
            file.seek(blk.number() * (long) blocksize);
            byte[] buffer = new byte[blocksize];
            file.readFully(buffer);
            java.lang.foreign.MemorySegment.copy(buffer, 0, p.contents(), ValueLayout.JAVA_BYTE, 0, blocksize);
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + blk);
        }
    }

    public synchronized void write(BlockId blk, Page p) {
        try {
            RandomAccessFile file = getFile(blk.fileName());
            file.seek(blk.number() * (long) blocksize);
            byte[] buffer = p.toByteArray();
            file.write(buffer);
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
            RandomAccessFile file = getFile(filename);
            return (int) (file.length() / blocksize);
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

    private RandomAccessFile getFile(String filename) throws IOException {
        RandomAccessFile file = openFiles.get(filename);
        if (file == null) {
            File dbTable = new File(dbDirectory, filename);
            file = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, file);
        }
        return file;
    }

    public Arena arena() {
        return arena;
    }
}
