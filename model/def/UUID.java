package ru.eludia.base.model.def;

public class UUID extends Def {

    @Override
    public Object getValue () {
        return java.util.UUID.randomUUID ();
    }
        
}
