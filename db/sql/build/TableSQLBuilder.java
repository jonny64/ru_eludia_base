package ru.eludia.base.db.sql.build;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import ru.eludia.base.model.Col;
import ru.eludia.base.model.Table;
import ru.eludia.base.model.phys.PhysicalCol;
import ru.eludia.base.db.util.ParamSetter;

/**
 * Буфер для генерации DML-запросов, связанных с 
 * определённой таблицей.
 */

public abstract class TableSQLBuilder extends SQLBuilder {
    
    Table table;

    public TableSQLBuilder (Table table, Set<String> fields) {
        
        this.table = table;
        
        List <PhysicalCol> [] isPk2colList = new List [] {null, null};
        
        for (int i = 0; i < isPk2colList.length; i ++) isPk2colList [i] = new ArrayList <> ();
        
        for (String s: fields) {            

            Col column = table.getColumn (s);

            if (column == null) continue;
            
            int i = column.isPk () ? 1 : 0;
            
            if (isPk2colList [i] == null) isPk2colList [i] = new ArrayList<> ();
            
            isPk2colList [i].add (column.toPhysical ());
        
        }
        
        if (isPk2colList [0] != null) {
            cols = isPk2colList [0];
            if (isPk2colList [1] != null) cols.addAll (isPk2colList [1]);
        }
        else {
            cols = isPk2colList [1];
        }
        
    }

    public Table getTable () {
        return table;
    }

    protected final void setParams (PreparedStatement st, ParamSetter ps, Map<String, Object> r) throws SQLException {
        
        openLogRecord ();

        for (int n = 1; n <= cols.size (); n ++) {

            PhysicalCol col = cols.get (n - 1);

            Object value = r.get (col.getName ());
            
            ps.setParam (st, n, col.getType (), value);
            
            logParam (value);

        }
        
        closeLogRecord ();
            
    }

}