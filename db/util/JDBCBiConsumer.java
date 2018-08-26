package ru.eludia.base.db.util;

@FunctionalInterface
public interface JDBCBiConsumer<T, U> {
    
    void accept (T t, U u) throws java.sql.SQLException;
    
}
