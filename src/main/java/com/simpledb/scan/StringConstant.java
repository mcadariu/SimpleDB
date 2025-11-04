package com.simpledb.scan;

public record StringConstant(String val) implements Constant {
    @Override
    public Object asJavaVal() {
        return val;
    }

    @Override
    public String asString() {
        return val;
    }

    @Override
    public int asInt() {
        return Integer.parseInt(val);
    }

    @Override
    public String toString() {
        return val;
    }

    @Override
    public int compareTo(Constant other) {
        StringConstant sc = (StringConstant) other;
        return val.compareTo(sc.val);
    }
}
