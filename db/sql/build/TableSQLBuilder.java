package ru.eludia.base.db.sql.build;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import ru.eludia.base.model.Col;
import ru.eludia.base.model.Table;
import ru.eludia.base.model.phys.PhysicalCol;
import ru.eludia.base.db.util.ParamSetter;
import ru.eludia.base.model.abs.AbstractCol;

/**
 * Буфер для генерации DML-запросов, связанных с 
 * определённой таблицей.
 */

public abstract class TableSQLBuilder extends SQLBuilder {
    
    Table table;
    Collection<String> keyColNames;
    List <PhysicalCol> keyCols;
    List <PhysicalCol> nonKeyCols;
    
    public boolean isInKey (AbstractCol c) {
        return keyColNames.contains (c.getName ());
    }

    public List<PhysicalCol> getKeyCols () {
        return keyCols;
    }

    public List<PhysicalCol> getNonKeyCols () {
        return nonKeyCols;
    }
    
    private static Collection<String> adjustKeyColNames (Table table, Collection<String> keyColNames) {

        if (keyColNames == null) return Collections.EMPTY_LIST;

        if (keyColNames.isEmpty ()) return table.getKeyColNames ();
        
        return keyColNames;
        
    }
    
    public TableSQLBuilder (Table table, Set<String> colNames, Collection<String> keyColNames) {

        super ();

        this.table       = table;
        this.keyColNames = adjustKeyColNames (table, keyColNames);

        cols = new ArrayList <> (colNames.size ());
        
        for (String i: colNames) {            
            Col column = table.getColumn (i);
            if (column == null) continue;
            if (isInKey (column)) continue;
            final PhysicalCol phy = column.toPhysical ();
            if (!cols.contains (phy)) cols.add (phy);
        }
        
        final int n = cols.size ();
        
        for (String i: this.keyColNames) {            
            Col column = table.getColumn (i);
            if (column == null) throw new IllegalArgumentException ("Key column " + i + " not found in " + table.getName ());
            cols.add (column.toPhysical ());
        }
        
        nonKeyCols = cols.subList (0, n);
        keyCols    = cols.subList (n, cols.size ());
        
    }

    public Table getTable () {
        return table;
    }

    protected final void setParams (PreparedStatement st, ParamSetter ps, Map<String, Object> r) throws SQLException {
        
        openLogRecord ();

        for (int n = 1; n <= cols.size (); n ++) {

            PhysicalCol col = cols.get (n - 1);

            Object value = r.get (col.getName ());
            
            ps.setParam (st, n, col.getType (), col.getLength (), value);
            
            logParam (value);

        }
        
        closeLogRecord ();
            
    }

}