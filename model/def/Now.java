package ru.eludia.base.model.def;

public final class Now extends Def {
        
    @Override
    public Object getValue () {
        return new java.sql.Timestamp (System.currentTimeMillis ());
    }
        
}
