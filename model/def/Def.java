package ru.eludia.base.model.def;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Экземпляры этого класса соответствуют значениям из раздела
 * DEFAULT в определении столбца таблицы (при CREATE TABLE и т. п.):
 * как константам, так и некоторым функциям.
 * 
 * Их основное назначение - присутствовать в модели данных, предоставляя
 * данные для генерации DDL.
 *  
 * Эти значения могут использоваться и в качестве параметров SQL-запросов,
 * что в основном имеет смысл для функций.
 */
public abstract class Def {
    
    /**
     * Глобальная переменная-генератор значений текущего времени.
     * В основном для определения полей "дата/время создания записи".
     */
    public static final Now  NOW      = new Now  ();    

    /**
     * Глобальная переменная-генератор UUID.
     * Для определения полей, автоматически помечающих записи случайными UUID.
     */
    public static final UUID NEW_UUID = new UUID ();
    
    /**
     * Генератор значений для подстановки в качестве параметров запросов
     * @return Значение, подходящее для PfreparedStatement.setXXX
     */
    public abstract Object getValue ();
    
    public static final Def valueOf (Object o) {
        if (o == null) return null;
        if (o instanceof Def) return (Def) o;
        if (o instanceof Boolean) return Bool.valueOf ((Boolean) o);
        if (o instanceof BigInteger) return Num.valueOf ((BigInteger) o);
        if (o instanceof BigDecimal) return Num.valueOf ((BigDecimal) o);
        return null;
    }
    
}