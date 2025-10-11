package com.simpledb.scan;

public class IntConstant implements Constant {
    private Integer val;

    public IntConstant(int val) {
        this.val = val;
    }

    @Override
    public Object asJavaVal() {
        return val;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        IntConstant that = (IntConstant) obj;
        return val.equals(that.val);
    }

    @Override
    public int hashCode() {
        return val.hashCode();
    }

    @Override
    public String toString() {
        return val.toString();
    }

    @Override
    public int compareTo(Constant other) {
        IntConstant ic = (IntConstant) other;
        return val.compareTo(ic.val);
    }
}
