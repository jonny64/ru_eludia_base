package ru.eludia.base.model;

import ru.eludia.base.model.abs.NamedObject;

public class Trigger extends NamedObject {
    
    String when;
    String what;

    public Trigger (String name, String when, String what) {
        super (name);
        this.when = when;
        this.what = what;
    }

    public Trigger (String when, String what) {
        this (when.replaceAll (" ", "_").toLowerCase (), when, what);
    }

    public String getWhat () {
        return what;
    }

    public String getWhen () {
        return when;
    }
    
}