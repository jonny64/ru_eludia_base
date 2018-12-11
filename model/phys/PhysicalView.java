package ru.eludia.base.model.phys;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

public final class PhysicalView extends PhysicalTable {
        
    final Logger logger = Logger.getLogger (this.getClass ().getName ());
    
    String sql;

    public String getSql () {
        return sql;
    }

    public PhysicalView (ResultSet rs) throws SQLException {        
        super (rs);
        sql = rs.getString ("TEXT");
    }

}