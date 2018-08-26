package ru.eludia.base.model.abs;

import java.util.ArrayList;
import java.util.List;
import ru.eludia.base.model.Trigger;

public abstract class AbstractTable<C extends AbstractCol, K extends AbstractKey> extends NamedObject {
    
    protected Roster<C> columns        = new Roster<> ();
    protected Roster<K> keys           = new Roster<> ();
    protected Roster<Trigger> triggers = new Roster<> ();
    List<C> pk = null;

    public final Roster<C> getColumns () {
        return columns;
    }

    public Roster<K> getKeys () {
        return keys;
    }

    public Roster<Trigger> getTriggers () {
        return triggers;
    }

    public AbstractTable (String name) {
        super (name);
    }
    
    public AbstractTable (String name, String remark) {
        super (name, remark);
    }
    
    public C getColumn (String name) {
        return columns.get (name);
    }
    
    public List<C> getPk () {
        return pk;
    }
    
    public String [] getPkColNames () {
        final int size = pk.size ();
        String [] result = new String [size];
        for (int i = 0; i < size; i++) result [i] = pk.get (i).getName ();
        return result;
    }
    
    public final void pk (C c, String... aliases) {
        add (c, aliases);
        if (pk == null) pk = new ArrayList<> (1);
        c.setPkPos (pk.size ());
        pk.add (c);
    }
    
    public final void add (C c, String... aliases) {
        columns.add (c, aliases);
    }

    public final void add (K k) {
        keys.add (k);
    }
    
}