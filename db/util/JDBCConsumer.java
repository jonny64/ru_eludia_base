package ru.eludia.base.db.util;

@FunctionalInterface
public interface JDBCConsumer<T> {
    
    void accept (T t) throws java.sql.SQLException;
    
}
