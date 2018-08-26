package ru.eludia.base.model;

public abstract class View extends Table {
        
    public View (String name) {
        super (name);
    }
    
    public View (String name, String remark) {
        super (name, remark);
    }
        
    public abstract String getSQL ();
    
}