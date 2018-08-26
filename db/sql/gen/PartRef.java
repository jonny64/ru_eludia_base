package ru.eludia.base.db.sql.gen;

import ru.eludia.base.model.Ref;

public class PartRef {
    
    Part part;
    Ref  ref;

    public PartRef (Part part, Ref ref) {
        this.part = part;
        this.ref = ref;
    }

    public Part getPart () {
        return part;
    }

    public Ref getRef () {
        return ref;
    }

    @Override
    public String toString () {
        return "[" + getPart ().getTableAlias () + "." + getRef () + "]";
    }
    
}
