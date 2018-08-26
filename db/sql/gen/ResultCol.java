package ru.eludia.base.db.sql.gen;

import ru.eludia.base.model.Col;
import ru.eludia.base.model.Table;
import ru.eludia.base.model.phys.PhysicalCol;

/**
 * Столбец выборки (результата SELECT)
 */
public class ResultCol {
    
    PhysicalCol col;
    String alias = null;

    private static final String AS = " AS ";

    /**
     * Конструктор для столбца, 100% соответствующего полю таблицы (а не произвольному выражению)
     * @param t Описание таблицы
     * @param s Имя поля либо строка "(имя) AS (псевдоним)"
     */
    public ResultCol (Table t, String s) {
        
        String name = s;
        
        int idx = s.indexOf (AS);
        
        if (idx >= 0) {
            name  = s.substring (0, idx);
            alias = s.substring (idx + AS.length ());
        }
                
        Col column = t.getColumn (name);
        
        if (column == null) throw new IllegalArgumentException ("Column '" + name + "' not found in " + t.getName ());
        
        col = column.toPhysical ();
        
    }

    /**
     *
     * @return Описание извлекаемого столбца
     */
    public final PhysicalCol getCol () {
        return col;
    }

    /**
     *
     * @return Псевдоним, под которым должен фигурировать данный столбец в выборке,
     * если таковой задан явно. А если не задан (и псевдоним должен вычисляться
     * внешним генератором SQL)
     */
    public final String getAlias () {
        return alias;
    }    
    
    /**
     *
     * @return Имя извлекаемого столбца в его таблице
     */
    public final String getName () {
        return col.getName ();
    }

    @Override
    public String toString () {
        return "[" + col + " AS " + getAlias () + "]";
    }

}