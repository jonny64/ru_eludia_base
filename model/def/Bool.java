package ru.eludia.base.model.def;

public final class Bool extends Num {
    
    public static final Bool TRUE = new Bool (1);
    public static final Bool FALSE = new Bool (0);
    
    protected Bool (int value) {
        super (value);
    }
    
}
