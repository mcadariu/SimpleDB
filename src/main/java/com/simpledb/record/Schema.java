package com.simpledb.record;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Schema {
    private List<String> fields = new ArrayList<>();
    private Map<String, FieldInfo> info = new HashMap<>();

    public void addField(String fldname, int type, int length) {
        fields.add(fldname);
        info.put(fldname, new FieldInfo(type, length));
    }

    public void addIntField(String fldname) {
        addField(fldname, Types.INTEGER, 0);
    }

    public void addStringField(String fldnane, int length) {
        addField(fldnane, Types.VARCHAR, length);
    }

    public void add(String fldname, Schema sch) {
        int type = sch.type(fldname);
        int length = sch.length(fldname);
        addField(fldname, type, length);
    }

    public void addAll(Schema schema) {
        for (String fldname : schema.fields())
            add(fldname, schema);
    }

    public List<String> fields() {
        return fields;
    }

    public boolean hasField(String fldname) {
        return fields.contains(fldname);
    }

    public int type(String fldname) {
        return info.get(fldname).type;
    }

    public int length(String fldname) {
        return info.get(fldname).length;
    }

    class FieldInfo {
        int type, length;

        public FieldInfo(int type, int length) {
            this.type = type;
            this.length = length;
        }
    }

}
