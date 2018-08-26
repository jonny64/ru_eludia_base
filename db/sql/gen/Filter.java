package ru.eludia.base.db.sql.gen;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import ru.eludia.base.model.Col;
import ru.eludia.base.model.Table;

/**
 * Фильтр для Select
 */
public class Filter {
    
    Col column;
    Predicate predicate;
    boolean forJoinOnly = false;
    
    private static final Pattern RE = Pattern.compile ("^([a-z][a-z0-9_]*)\\s*(.*)$");

    /**
     * Установить признак локальности фильтра
     * @param forJoinOnly см. isForJoinOnly ()
     */
    public final void setForJoinOnly (boolean forJoinOnly) {
        this.forJoinOnly = forJoinOnly;
    }

    /**
     * Признак того, что этот фильтр НЕ превращает LEFT JOIN в INNER JOIN
     * @return true, если фильтр ограничивает множество записей внутри LEFT JOIN; 
     * false, если, напротив, фильтр следует понимать на уровне общего WHERE.
     */
    public final boolean isForJoinOnly () {
        return forJoinOnly;
    }

    public Filter (Col column, Predicate predicate) {
        this.column = column;
        this.predicate = predicate;
    }
    
    public Filter (Table t, String name, Predicate predicate) {
        this.column = t.getColumn (name);
        if (this.column == null) throw new IllegalArgumentException ("No column " + name + " found in " + t.getName ());
        this.predicate = predicate;
    }

    public Filter (Table t, String src, Object[] values) {
        
        Matcher matcher = RE.matcher (src);
        
        if (!matcher.matches ()) throw new IllegalArgumentException ("Invalid filter: " + src);
        
        column = t.getColumn (matcher.group (1));
        
        if (column == null) throw new IllegalArgumentException ("Column not found in " + t.getName () + ": " + matcher.group (1));
        
        predicate = new Predicate (matcher.group (2), values);

    }
    
    /**
     * Признак того, что данный фильтр следует игнорировать при генерации SQL.
     * 
     * В Web-интерфейсах в поисковых запросах пустое значение параметра обычно 
     * означает отсутствие ограничения на соответствующее поле.
     * 
     * @return true, если число параметров не 0 (то есть условие не IS NULL) 
     * и 1-й параметр имеет значение null.
     */
    public final boolean isOff () {
        return predicate.isOff ();
    }

    /**
     * По какому полю этот фильтр
     * @return описание поля таблицы
     */
    public Col getColumn () {
        return column;
    }

    /**
     * Условие, наложенное на поле getColumn данным фильтром
     * @return суть условия
     */
    public Predicate getPredicate () {
        return predicate;
    }
        
}