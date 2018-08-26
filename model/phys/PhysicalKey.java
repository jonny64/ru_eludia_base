package ru.eludia.base.model.phys;

import java.util.ArrayList;
import java.util.List;
import ru.eludia.base.model.abs.AbstractKey;

public class PhysicalKey extends AbstractKey {
    
    List<PhysicalKeyPart> parts = new ArrayList<> ();
    
    public PhysicalKey (String name) {
        super (name);
    }

    public List<PhysicalKeyPart> getParts () {
        return parts;
    }
    
    @Override
    public boolean equals (Object obj) {

        if (!(obj instanceof PhysicalKey)) return false;
        
        PhysicalKey that = (PhysicalKey) obj;
        
        return 
            this.isUnique () == that.isUnique () &&
            this.getName  ().equals (that.getName  ()) &&
            this.getParts ().equals (that.getParts ()) ;
        
    }

    @Override
    public String toString () {
        return (isUnique () ? "{UNIQUE " :  "{NONUNIQUE ") + name + ": " + parts + "}";
    }
    
}