package ru.eludia.base.db.util;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface ParamSetter {
        
    void setParam (PreparedStatement st, int n, JDBCType type, int length, Object value) throws SQLException;    
    
}
