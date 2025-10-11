package com.simpledb.record;

import com.simpledb.file.Page;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class Layout {
    private Schema schema;
    private Map<String, Integer> offsets;
    private int slotsize;

    public Layout(Schema schema) {
        this.schema = schema;
        offsets = new HashMap<>();
        int pos = Integer.BYTES;

        for (String fldname : schema.fields()) {
            offsets.put(fldname, pos);
            pos += lengthInBytes(fldname);
        }

        slotsize = pos;
    }

    public Layout(Schema schema, Map<String, Integer> offsets, int slotsize) {
        this.schema = schema;
        this.offsets = offsets;
        this.slotsize = slotsize;
    }

    public Schema schema() {
        return schema;
    }

    public int offset(String fldname) {
        return offsets.get(fldname);
    }

    public int slotsize() {
        return slotsize;
    }

    private int lengthInBytes(String fldname) {
        int fldtype = schema.type(fldname);

        if (fldtype == Types.INTEGER) {
            return Integer.BYTES;
        } else {
            return Page.maxLength(schema.length(fldname));
        }
    }
}
