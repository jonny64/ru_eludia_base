package ru.eludia.base.db.sql.gen;

import ru.eludia.base.model.Col;
import ru.eludia.base.model.Ref;
import ru.eludia.base.model.Table;

public final class JoinToOne extends Join {

    public JoinToOne (Select select, Table table, String... names) {
        super (select, table, names);
    }

    private Ref getRef (Part p, String name) {
        
        if (!name.isEmpty ()) {
            
            Col column = p.getTable ().getColumn (name);
            
            if (column == null) throw new IllegalArgumentException (p.getTable ().getName () + "." +  name + " not found");

            if (!(column instanceof Ref)) throw new IllegalArgumentException (p.getTable ().getName () + "." +  name + " is not a reference");
            
            return (Ref) column;
            
        }
        
        for (Col column: p.getTable ().getColumns ().values ()) {
            
            if (!(column instanceof Ref)) continue;
            
            Ref ref = (Ref) column;
            
            if (ref.getTargetTable ().equals (getTable ())) return ref;

        }
        
        return null;
        
    }

    @Override
    public Select on (String hint) {
        
        if (hint == null) hint = "";
        
        if (hint.contains ("=")) {
            joinCondition = new JoinConditionBySrc (hint);
            return select;
        }
        
        String [] ar = hint.split (".");
        
        String alias = ar.length < 1 || ar [0] == null ? "" : ar [0];
        String refnm = ar.length < 2 || ar [1] == null ? "" : ar [1];
        
        Ref ref   = null;
        Part part = null;
        
        if (alias.isEmpty ()) {
            ref = getRef (select, refnm);
            if (ref != null) part = select;
        }
        
        if (ref == null) {            
            for (Join j: select.joins) {
                ref = getRef (j, refnm);
                if (ref == null) continue;
                part = j;
                break;
            }            
        }

        if (ref == null) throw new IllegalArgumentException ("reference to " + table.getName () +  " not found");
        
        joinCondition = new JoinConditionByRef (new PartRef (part, ref), this);
                        
        return select;
        
    }
            
}