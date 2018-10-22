package ru.eludia.base.model.abs;

public abstract class AbstractCol extends NamedObject {
    
    protected boolean nullable = false;
    protected int length = 0;
    protected int precision = 0;
    
    public boolean isNullable () {
        return nullable;
    }

    public void setNullable (boolean nullable) {
        this.nullable = nullable;
    }

    public int getLength () {
        return length;
    }

    public int getLength (int def) {
        return length > 0 ? length : def;
    }

    public int getPrecision () {
        return precision;
    }
    
    public int getPrecision (int def) {
        return precision > 0 ? precision : def;
    }

    public AbstractCol (String name, String remark) {        
        super (name);
        if (remark != null) this.remark = remark;
    }

    public AbstractCol (String name, int length, String remark) {
        this (name, remark);
        this.length = length;
    }
    
    public AbstractCol (String name, int length, int precision, String remark) {
        this (name, remark);
        this.length = length;
        this.precision = precision;
    }

    public AbstractCol (String name, int length, int precision) {
        super (name);
        this.length = length;
        this.precision = precision;
    }

}