package com.simpledb.scan;

public class StringConstant implements Constant {
    private String val;

    public StringConstant(String val) {
        this.val = val;
    }

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
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        StringConstant that = (StringConstant) obj;
        return val.equals(that.val);
    }

    @Override
    public int hashCode() {
        return val.hashCode();
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
