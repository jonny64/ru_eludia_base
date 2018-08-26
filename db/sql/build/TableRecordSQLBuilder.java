package ru.eludia.base.db.sql.build;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import ru.eludia.base.model.Table;
import ru.eludia.base.db.util.ParamSetter;

/**
 * Буфер для генерации DML-запросов, связанных с 
 * определённой таблицей, и их запуска в применении 
 * к отдельной записи.
 */

public class TableRecordSQLBuilder extends TableSQLBuilder {

    Map <String, Object> record;

    public TableRecordSQLBuilder (Table table, Map<String, Object> record) {
        super (table, record.keySet ());
        this.record = record;
    }

    @Override
    public final void setParams (PreparedStatement st, ParamSetter ps) throws SQLException {        
        setParams (st, ps, record);
    }
    
}