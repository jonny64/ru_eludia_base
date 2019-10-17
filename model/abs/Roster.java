package ru.eludia.base.model.abs;

import java.util.concurrent.ConcurrentHashMap;

public final class Roster<T extends NamedObject> extends ConcurrentHashMap<String, T> {
    
    ConcurrentHashMap<String, T> aliases = new ConcurrentHashMap <> ();
    
    private static String toCanonical (String s) {
        return s.toUpperCase ().replace ("_", "");
    }

    @Override
    public T put (String key, T value) {
        if (value == null) throw new IllegalArgumentException ("null values are not allowed");
        if (aliases.containsKey (key)) throw new IllegalArgumentException (key + " is already set");
        aliases.put (key, value);
        aliases.put (toCanonical (key), value);
        return super.put (key, value); 
    }

    @Override
    public T get (Object key) {
        if (aliases.containsKey (key)) return aliases.get (key);
        return aliases.get (toCanonical (key.toString ()));
    }

    @Override
    public boolean containsKey (Object key) {
        if (aliases.containsKey (key)) return true;
        if (aliases.containsKey (toCanonical (key.toString ()))) return true;
        return false;
    }
    
    public void alias (String key, String alias) {
        T v = get (key);
        if (v == null) throw new IllegalArgumentException (key + " not found");
        aliases.put (alias, v);
        aliases.put (toCanonical (alias), v);
    }
    
    public void add (T o, String... a) {
        String key = o.getName ();
        put (key, o);
        for (String alias: a) alias (key, alias);
    }

    public void remove (String key) {
        super.remove (key); 
        aliases.remove (key);
        aliases.remove (toCanonical (key));
    }

}