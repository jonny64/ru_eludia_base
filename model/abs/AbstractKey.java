package ru.eludia.base.model.abs;

public abstract class AbstractKey extends NamedObject {
    
    boolean unique = false;

    public AbstractKey (String name) {
        super (name);
    }

    public final void setUnique (boolean unique) {
        this.unique = unique;
    }

    public final boolean isUnique () {
        return unique;
    }

}