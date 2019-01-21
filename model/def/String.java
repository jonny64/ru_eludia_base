package ru.eludia.base.model.def;

public class String extends Const {

    java.lang.String value;

    public String (java.lang.String value) {
        this.value = value;
    }

    @Override
    public java.lang.String toSql () {
        return "'" + value + "'";
    }

    @Override
    public Object getValue () {
        return value;
    }
    
}