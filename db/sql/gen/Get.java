package ru.eludia.base.db.sql.gen;

import java.util.List;
import ru.eludia.base.model.Col;
import ru.eludia.base.model.Table;

/**
 * Генератор SELECT на поиск записи заданной таблицы по первичному ключу
 */
public class Get extends Select {

    /**
     * Конструктор
     * @param table Описание таблицы
     * @param id Первичный ключ записи
     * @param names "AS псевдоним" + поля
     */
    public Get (Table table, Object id, String... names) {
        
        super (table, names);
        
        final List<Col> pk = table.getPk ();
        
        if (pk.size () != 1) throw new IllegalArgumentException ("This Get constructor is for scalar PK only");
        
        where (pk.get (0).getName (), id);

    }

}