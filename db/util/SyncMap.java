package ru.eludia.base.db.util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import ru.eludia.base.DB;
import static ru.eludia.base.DB.HASH;
import ru.eludia.base.db.sql.gen.Select;
import ru.eludia.base.model.Col;
import ru.eludia.base.model.Table;

public abstract class SyncMap<T> extends HashMap<String, Map<String, Object>> {
    
    protected final Logger logger = Logger.getLogger (this.getClass ().getName ());
    
    protected DB db;
    protected Map<String, Object> commonPart = HASH ();

    public SyncMap (DB db) {
        this.db = db;
    }
    
    public abstract String [] getKeyFields ();
    public abstract void setFields (Map<String, Object> h, T o);
    public abstract Table getTable ();
        
    public void reload () throws SQLException {        
        clear ();
        Select select = db.getModel ().select (getTable (), "*");        
        commonPart.forEach ((k, v) -> {select.and (k, v);});        
        db.forEach (select, (rs) -> {addRecord (db.HASH (rs));});
    }
    
    public void processCreated (List<Map<String, Object>> created) throws SQLException {}
    public void processUpdated (List<Map<String, Object>> updated) throws SQLException {}
    public void processDeleted (List<Map<String, Object>> deleted) throws SQLException {}
    
    public void addRecord (Map<String, Object> h) {
        put (getKey (h), h);
    }
    
    public void addAll (Collection<T> l) {
        addAll (l, Collections.EMPTY_MAP);
    }
    
    public void addAll (Collection<T> l, Map<String, Object> parent) {
        for (T o: l) add (o, parent);
    }
    
    public void add (T o, Map<String, Object> parent) {
        Map<String, Object> h = HASH ();
        h.putAll (parent);
        setFields (h, o);
        addRecord (h);
    }

    public Object getPk (T o) {
        final Map<String, Object> r = toRecord (o);        
        return r == null ? null : r.get (getTable ().getPk ().get (0).getName ());
    }

    public Map<String, Object> toRecord (T o) {
        return o == null ? null : get (getKey (o));
    }

    public String getKey (T o) {
        Map<String, Object> h = HASH ();
        setFields (h, o);
        return getKey (h);
    }
    
    public String getKey (Map<String, Object> h) {
        
        StringBuilder sb = new StringBuilder ();
        
        for (String i: getKeyFields ()) {
            
            final Object o = h.get (i);
            
            if (o == null) throw new IllegalArgumentException ("Key field " + i + " is null: " + h);
            
            if (sb.length () > 0) sb.append ('_');
            
            String s = o.toString ();
            
            switch (getTable ().getColumn (i).getType ()) {
                case DATE:
                    s = s.substring (0, 10);
                    break;
                default:
            }
            
            sb.append (s);
            
        }
        
        return sb.toString ();
        
    }
    
    public void sync () throws SQLException {
        
        List <Map<String, Object>> records = new ArrayList (size ());
        
        for (Map<String, Object> i: values ()) {
            i.putAll (commonPart);
            records.add (i);
        }
        
        HashMap<String, Map<String, Object>> wanted = new HashMap<> ();
        wanted.putAll (this);
        logger.info ("wanted=" + wanted);
        
        reload  ();
        
        HashMap<String, Map<String, Object>> old = new HashMap<> ();
        old.putAll (this);
        logger.info ("old=" + old);
        
        ArrayList<String> key = new ArrayList<> (getKeyFields ().length + commonPart.size ());
        key.addAll (Arrays.asList (getKeyFields ()));
        key.addAll (commonPart.keySet ());
        
        db.upsert (getTable (), records, key.toArray (getKeyFields ()));
        
        reload  ();
        logger.info ("this=" + this);
        
        List<Map<String, Object>> created = new ArrayList ();
        List<Map<String, Object>> updated = new ArrayList ();
        List<Map<String, Object>> deleted = new ArrayList ();        
        
        for (String k: keySet ()) {
            
            Map<String, Object> r = get (k);
            
            if (!wanted.containsKey (k)) {
                deleted.add (r);
            }
            else if (!old.containsKey (k)) {
                r.put (WANTED, wanted.get (k));
                created.add (r);
            }
            else {
                r.put (ACTUAL, old.get (k));
                r.put (WANTED, wanted.get (k));
                updated.add (r);
            }
            
        }

        logger.info ("created=" + created);
        processCreated (created);
        
        logger.info ("updated=" + updated);
        processUpdated (updated);
        
        logger.info ("deleted=" + deleted);
        processDeleted (deleted);
        
    }
    
    public static final String WANTED = "\twanted";
    public static final String ACTUAL = "\tactual";
    
}
