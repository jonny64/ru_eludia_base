package ru.eludia.base.model;

import ru.eludia.base.model.abs.NamedObject;

public class Procedure extends NamedObject {
    
    String params;
    String body;

    public Procedure (String name, String body) {
        this (name, "", body);
    }

    public Procedure (String name, String params, String body) {
        super (name);
        this.params = params;
        this.body = body;
    }

    public String getParams () {
        return params;
    }

    public String getBody () {
        return body;
    }
    
}