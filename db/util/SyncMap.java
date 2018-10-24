package ru.eludia.base.db.util;

import java.sql.SQLException;
import java.util.ArrayList;
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
    
    public void addAll (List<T> l) {
        for (T o: l) add (o);
    }
    
    public void add (T o) {
        Map<String, Object> h = HASH ();
        setFields (h, o);
        addRecord (h);
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
        
        List <String> virtualColNames = new ArrayList ();
        for (Col c: getTable ().getColumns ().values ()) if (c.toPhysical ().isVirtual ()) virtualColNames.add (c.getName ());
        
        for (Map<String, Object> i: values ()) {
            i.putAll (commonPart);
            for (String v: virtualColNames) i.remove (v);
            records.add (i);
        }
        
        HashMap<String, Map<String, Object>> wanted = new HashMap<> ();
        wanted.putAll (this);
        logger.info ("wanted=" + wanted);
        
        reload  ();
        
        HashMap<String, Map<String, Object>> old = new HashMap<> ();
        old.putAll (this);
        logger.info ("old=" + old);
        
        db.upsert (getTable (), records, getKeyFields ());
        
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
