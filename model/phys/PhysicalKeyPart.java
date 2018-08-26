package ru.eludia.base.model.phys;

import ru.eludia.base.model.phys.PhysicalCol;

public class PhysicalKeyPart {
    
    PhysicalCol col;    
    boolean isDesc = false;

    public PhysicalKeyPart (PhysicalCol col) {
        this.col = col;
    }

    public PhysicalCol getCol () {
        return col;
    }

    public boolean isDesc () {
        return isDesc;
    }

    public void setIsDesc (boolean isDesc) {
        this.isDesc = isDesc;
    }
    
    @Override
    public boolean equals (Object obj) {
        
        if (!(obj instanceof PhysicalKeyPart)) return false;
        
        PhysicalKeyPart that = (PhysicalKeyPart) obj;
        
        return 
            this.getCol ().getName  ().toUpperCase ().equals (that.getCol ().getName ().toUpperCase ()) &&
            this.isDesc == that.isDesc;
        
    }

    @Override
    public String toString () {
        return "[" + col.getName () + (isDesc ? " DESC]" : " ASC]");
    }
    
}
