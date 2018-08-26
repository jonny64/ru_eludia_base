package ru.eludia.base.db.sql.gen;

import java.util.Set;
import java.util.HashSet;
import ru.eludia.base.model.Col;
import ru.eludia.base.model.Ref;
import ru.eludia.base.model.Table;

public final class JoinToMany extends Join {

    public JoinToMany (Select select, Table table, String... names) {
        super (select, table, names);
    }

    private Part getPartByTable (Table t) {
        if (t.equals (select.getTable ())) return select;
        for (Join j: select.joins) {
            if (j.equals (this)) break;
            if (t.equals (j.table)) return j;
        }
        throw new IllegalArgumentException ("No referenced part found: " + t.getName ());
    }
    
    private Ref getFirstRef () {
        
        Set<Table> prevTables = new HashSet<> ();
        
        prevTables.add (select.table);
        
        for (Join j: select.joins) {
            if (j.equals (this)) break;
            prevTables.add (j.table);
        }
        
        for (Col i: table.getColumns ().values ()) {
            
            if (!(i instanceof Ref)) continue;
            
            Ref r = (Ref) i;
            
            if (prevTables.contains (r.getTargetTable ())) return r;
            
        }
        
        throw new IllegalArgumentException ("No ref poinping to " + prevTables + " found in " + table.getName ());
        
    }    

    @Override
    public Select on (String hint) {
        
        if (hint == null) hint = "";
        
        String [] rt = hint.split (" ");

        PartRef pr = new PartRef (this, 
            rt.length > 0 && !rt [0].isEmpty () ? 
                (Ref) table.getColumn (rt [0]) : 
                getFirstRef ()
        );
        
        Part toPart = 
            rt.length > 1 ? 
                select.getPart (rt [1]) : 
                getPartByTable (pr.getRef ().getTargetTable ());
        
        joinCondition = new JoinConditionByRef (pr, toPart);
                        
        return select;
        
    }
            
}