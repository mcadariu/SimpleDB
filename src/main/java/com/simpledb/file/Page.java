package com.simpledb.file;

import java.lang.foreign.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    private final MemorySegment segment;
    private final Arena arena;
    public static final Charset CHARSET = StandardCharsets.US_ASCII;

    private static final ValueLayout.OfInt INT_LAYOUT = ValueLayout.JAVA_INT.withByteAlignment(4);
    private static final ValueLayout.OfByte BYTE_LAYOUT = ValueLayout.JAVA_BYTE;

    public Page(int blocksize, Arena arena) {
        this.arena = arena;
        this.segment = arena.allocate(blocksize, 1);
    }

    public Page(byte[] b, Arena arena) {
        this.arena = arena;
        this.segment = arena.allocate(b.length, 1);
        MemorySegment.copy(b, 0, segment, BYTE_LAYOUT, 0, b.length);
    }

    public int getInt(int offset) {
        return segment.get(INT_LAYOUT, offset);
    }

    public void setInt(int offset, int n) {
        segment.set(INT_LAYOUT, offset, n);
    }

    public byte[] getBytes(int offset) {
        int len = segment.get(INT_LAYOUT, offset);
        byte[] b = new byte[len];
        MemorySegment.copy(segment, BYTE_LAYOUT, offset + Integer.BYTES, b, 0, len);
        return b;
    }

    public void setBytes(int offset, byte[] b) {
        segment.set(INT_LAYOUT, offset, b.length);
        MemorySegment.copy(b, 0, segment, BYTE_LAYOUT, offset + Integer.BYTES, b.length);
    }

    public String getString(int offset) {
        byte[] b = getBytes(offset);
        return new String(b, CHARSET);
    }

    public void setString(int offset, String s) {
        byte[] b = s.getBytes(CHARSET);
        setBytes(offset, b);
    }

    public static int maxLength(int strlen) {
        int bytesNeeded = Integer.BYTES + strlen;
        // Round up to the next multiple of 4 to ensure proper alignment for subsequent int access
        return (bytesNeeded + 3) & ~3;
    }

    MemorySegment contents() {
        return segment;
    }

    Arena arena() {
        return arena;
    }

    public byte[] toByteArray() {
        byte[] b = new byte[(int) segment.byteSize()];
        MemorySegment.copy(segment, BYTE_LAYOUT, 0, b, 0, b.length);
        return b;
    }

    public void clear() {
        segment.fill((byte) 0);
    }
}
