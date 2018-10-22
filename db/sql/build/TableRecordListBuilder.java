package ru.eludia.base.db.sql.build;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import ru.eludia.base.model.Table;
import ru.eludia.base.db.util.ParamSetter;

/**
 * Буфер для генерации DML-запросов, связанных с 
 * определённой таблицей, и их запуска в применении 
 * к пакету записей.
 */

public class TableRecordListBuilder extends TableSQLBuilder {
    
    List<Map <String, Object>> records;

    public TableRecordListBuilder (Table table, List<Map<String, Object>> records, Collection<String> keyColNames) {
        super (table, records.isEmpty () ? Collections.EMPTY_SET : records.get (0).keySet (), keyColNames);
        this.records = records;
    }

    @Override
    public final void setParams (PreparedStatement st, ParamSetter ps) throws SQLException {
        
        if (records.isEmpty ()) return;
        if (cols.isEmpty ()) return;
        
        openLogRecord ();

        for (Map<String, Object> record: records) {
                        
            setParams (st, ps, record);                        
            
            st.addBatch ();
            
            psb.append (',');
                        
        }
        
        closeLogRecord ();

    }
    
    @Override
    public void execute (PreparedStatement st) throws SQLException {
        st.executeBatch ();
    }
    
}