package com.simpledb.scan;

public record IntConstant(int val) implements Constant {
    @Override
    public Object asJavaVal() {
        return val;
    }

    @Override
    public int asInt() {
        return val;
    }

    @Override
    public String asString() {
        return Integer.toString(val);
    }

    @Override
    public String toString() {
        return Integer.toString(val);
    }

    @Override
    public int compareTo(Constant other) {
        IntConstant ic = (IntConstant) other;
        return Integer.compare(val, ic.val);
    }
}
