package ru.eludia.base.db.sql.gen;

import java.util.StringTokenizer;
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

        Ref result = null;

        for (Col column: p.getTable ().getColumns ().values ()) {

            if (!(column instanceof Ref)) continue;

            Ref ref = (Ref) column;

            if (!ref.getTargetTable ().equals (getTable ())) continue;

            if (result != null) throw new IllegalArgumentException ("Ambiguous ref from " + p.getTable ().getName () + " to " + table.getName () + ": " + result.getName () + " vs. " + ref.getName ());

            result = ref;

        }

        return result;
        
    }

    @Override
    public Select on (String hint) {
        
        if (hint == null) hint = "";
        
        if (hint.contains ("=")) {
            joinCondition = new JoinConditionBySrc (hint);
            return select;
        }
        
        String alias = "";
        String refnm = "";
        StringTokenizer st = new StringTokenizer (hint, ".");
        
        while (st.hasMoreTokens ()) {            
            if (!alias.isEmpty ()) throw new IllegalArgumentException ("Invalid join hint: '" + hint + "'");
            if (!refnm.isEmpty ()) alias = refnm;
            refnm = st.nextToken ();
        }

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