package ru.eludia.base.model;

import ru.eludia.base.model.abs.AbstractKey;
import java.util.Arrays;
import java.util.List;

public class Key extends AbstractKey {
    
    List<String> parts;
    
    public Key (String name, String... parts) {
        super (name);
        this.parts = Arrays.asList (parts);
    }

    public List<String> getParts () {
        return parts;
    }

}