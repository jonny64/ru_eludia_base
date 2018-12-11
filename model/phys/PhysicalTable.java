package ru.eludia.base.model.phys;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;
import ru.eludia.base.model.abs.AbstractTable;

public class PhysicalTable extends AbstractTable<PhysicalCol, PhysicalKey> {
        
    final Logger logger = Logger.getLogger (this.getClass ().getName ());

    public PhysicalTable (ResultSet rs) throws SQLException {
        
        super (rs.getString ("TABLE_NAME"));
        
    }

}