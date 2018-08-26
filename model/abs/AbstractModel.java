package ru.eludia.base.model.abs;

import java.util.Collection;
import java.util.logging.Logger;

public abstract class AbstractModel <C extends AbstractCol, K extends AbstractKey, T extends AbstractTable<C, K>> {

    protected Roster<T> tables = new Roster<> ();
    
    protected final Logger logger = Logger.getLogger (this.getClass ().getName ());
    
    public final AbstractModel add (T t, String... a) {
        tables.add (t, a);
        tables.alias (t.getName (), t.getClass ().getName ());
//        logger.log (Level.INFO, "Registered {0} as {1}", new Object[]{t.getClass ().getName (), t.getName ()});
        return this;
    }
    
    public final Collection <T> getTables () {
        return tables.values ();
    }
    
    public final T get (Class c) {
        return get (c.getName ());
    }

    public final T get (String name) {
        T t = tables.get (name);
        return t;
    }
        
}