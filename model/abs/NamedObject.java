package ru.eludia.base.model.abs;

public abstract class NamedObject {
    
    protected String name;
    protected String remark = "";
    
    protected NamedObject (String name) {
        if (name == null) throw new IllegalArgumentException ("null name is not allowed");
        this.name = name;
    }

    protected NamedObject (String name, String remark) {
        this (name);
        setRemark (remark);
    }

    public final String getName () {
        return name;
    }
    
    public String getRemark () {
        return remark;
    }
    
    public void setRemark (String remark) {
        if (remark != null) this.remark = remark;
    }
    
}
