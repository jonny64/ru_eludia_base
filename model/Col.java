package ru.eludia.base.model;

import ru.eludia.base.model.abs.AbstractCol;
import ru.eludia.base.model.def.Def;
import ru.eludia.base.model.phys.PhysicalCol;

public class Col extends AbstractCol {
    
    Type type;    
    Def def = null;
    Table table;
    PhysicalCol physicalCol;
    
    public void setTable (Table table) {
        this.table = table;
    }

    public Def getDef () {
        return def;
    }

    public Type getType () {
        return type;
    }

    public void setDef (Def def) {
        nullable = (def == null);
        this.def = def;
    }

    public void setType (Type type) {
        this.type = type;
    }
    
    public Col (String name, Type type, String remark) {
        super (name, remark);
        setType (type);
    }
    
    public Col (String name, Type type, Def def, String remark) {
        this (name, type, remark);
        setDef (def);
    }

    public Col (String name, Type type, int length, String remark) {
        super (name, length, remark);
        setType (type);
    }
    
    public Col (String name, Type type, int length, Def def, String remark) {
        super (name, length, remark);
        setType (type);
        setDef (def);
    }

    public Col (String name, Type type, int length, int precision, String remark) {
        super (name, length, precision, remark);
        setType (type);
    }
    
    public Col (String name, Type type, int length, int precision, Def def, String remark) {
        super (name, length, precision, remark);
        setType (type);
        setDef (def);
    }
    
    public Table getTable () {
        return table;
    }
    
    @Override
    public String toString () {
        return "[" + getName () + " " + type + "(" + length + "," + precision + ")=" + def + (isNullable () ? " " : " NOT") + " NULL #" + remark + "]";
    }

    public PhysicalCol toPhysical () {
        return physicalCol;
    }

    public void setPhysicalCol (PhysicalCol physicalCol) {
        this.physicalCol = physicalCol;
    }

}