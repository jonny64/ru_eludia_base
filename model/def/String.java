package ru.eludia.base.model.def;

public class String extends Const {
    
    String value;

    public String (String value) {
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
