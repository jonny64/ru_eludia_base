package ru.eludia.base.model.def;

public class Blob extends Const {
    
    public final static Blob EMPTY_BLOB = new Blob ();

    @Override
    public java.lang.String toSql () {
        return "EMPTY_BLOB()";
    }

    @Override
    public Object getValue () {
        return "";
    }
        
}
