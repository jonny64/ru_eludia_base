package ru.eludia.base.model.def;

/**
 * Частный случай Def, представляющий константы (в отличие от функций).
 * 
 * Для каждого такого значения имеется фрагмент CREATE TABLE,
 * не зависящий от диалекта SQL.
 */
public abstract class Const extends Def {
    
    /**
     * SQL-предсталение данного значения
     * @return Строка для подстановки после CREATE TABLE ... (... DEFAULT
     */
    public abstract java.lang.String toSql ();
    
}
