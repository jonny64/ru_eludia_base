package ru.eludia.base.model;

import java.util.ArrayList;
import ru.eludia.base.model.abs.AbstractKey;
import java.util.List;

public class Key extends AbstractKey {
    
    List<String> parts;
    
    public Key (String name, Object... p) {
        super (name);
        final int len = p.length;
        parts = new ArrayList <> (len);
        for (int i = 0; i < len; i ++) parts.add (p [i].toString ().toLowerCase ());
    }

    public List<String> getParts () {
        return parts;
    }

}