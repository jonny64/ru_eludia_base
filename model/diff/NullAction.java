package ru.eludia.base.model.diff;

public enum NullAction {
    SET,
    UNSET;
    
    public final static NullAction get (boolean asIs, boolean toBe) {
        
        return
            asIs == toBe ? null  :
                    toBe ? SET   :
                           UNSET ;
        
    }
    
}