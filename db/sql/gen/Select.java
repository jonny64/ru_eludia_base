package ru.eludia.base.db.sql.gen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import ru.eludia.base.model.Table;

/**
 * Генератор плоских SELECT-запросов без группировки
 */
public class Select extends Part<Select> {

    List<Join> joins = Collections.EMPTY_LIST;
    StringBuilder order = null;
    int offset = 0;
    Integer limit = null;

    public Select (Table table, String... names) {
        super (table, names);
    }

    /**
     * Сдвиг 1-й записи выборки. Имеет смысл для запросов на листаемые списки.
     * @return ... а если листания нет, то 0.
     */
    public int getOffset () {
        return offset;
    }

    /**
     * Макисмальная длина выборки
     * @return null, если не определена (то есть запрос без листания).
     */
    public Integer getLimit () {
        return limit;
    }

    /**
     * Установка параметров листания
     * @param offset сдвиг 1-й записи
     * @param limit максимальная длина выборки
     * @return this
     */
    public Select limit (int offset, int limit) {
        this.offset = offset;
        this.limit = limit;
        return this;
    }

    /**
     * Текущий список JOIN-выражений
     * @return 
     */
    public List<Join> getJoins () {
        return joins;
    }

    /**
     * Установка ORDER BY либо приписывание дополнительного 
     * поля в конец списка
     * @param o что добавить
     * @return this
     */
    public final Select orderBy (String o) {
        
        if (order == null) {
            order = new StringBuilder (o);
        }
        else {
            order.append (',');
            order.append (o);
        }
        
        return this;
        
    }

    /**
     * Выражение для ORDER BY
     * @return SQL-код
     */
    public String getOrder () {
        return order == null ? null : order.toString ();
    }        
        
    /**
     * Низкоуровневое добавление JOIN-выражения.
     * 
     * Вместо этого метода следует использовать toXxx.
     * 
     * @param join что добавить
     */
    public void add (Join join) {
        if (joins.isEmpty ()) joins = new ArrayList <> ();
        joins.add (join);
    }
    
    Part getPart (String name) {
        if (name == null || name.isEmpty ()) return this;
        for (Join j: joins) if (name.equals (j.getTableAlias ())) return j;
        throw new IllegalArgumentException ("Part not found by name: " + name);
    }

    /**
     * Добавление INNER JOIN с таблицей дочерних записей:
     * то есть должно найтись 1 или более записей.
     * 
     * @param t Описание таблицы
     * @param names "AS псевдоним", список выбираемых полей
     * @return новый join
     */
    public JoinToMany toSeveral (Table t, String... names) {
        JoinToMany j = new JoinToMany (this, t, names);
        add (j);
        return j;
    }

    /**
     * Добавление INNER JOIN с таблицей дочерних записей:
     * то есть должно найтись 1 или более записей.
     * 
     * @param t Описание таблицы
     * @param names "AS псевдоним", список выбираемых полей
     * @return новый join
     */
    public JoinToMany toSeveral (String t, String... names) {
        return Select.this.toSeveral (table.getModel ().t (t), names);
    }

    /**
     * Добавление INNER JOIN с таблицей дочерних записей:
     * то есть должно найтись 1 или более записей.
     * 
     * @param t Описание таблицы
     * @param names "AS псевдоним", список выбираемых полей
     * @return новый join
     */
    public JoinToMany toSeveral (Class t, String... names) {
        return Select.this.toSeveral (table.getModel ().t (t), names);
    }
    
    /**
     * Добавление INNER JOIN с таблицей-справочником либо предком:
     * то есть должна найтись ровно 1 запись.
     * 
     * @param t Описание таблицы
     * @param names "AS псевдоним", список выбираемых полей
     * @return новый join
     */
    public JoinToOne toOne (Table t, String... names) {
        JoinToOne j = new JoinToOne (this, t, names);
        add (j);
        return j;
    }
    
    /**
     * Добавление INNER JOIN с таблицей-справочником либо предком:
     * то есть должна найтись ровно 1 запись.
     * 
     * @param t Описание таблицы
     * @param names "AS псевдоним", список выбираемых полей
     * @return новый join
     */
    public JoinToOne toOne (String t, String... names) {
        return toOne (table.getModel ().t (t), names);
    }

    /**
     * Добавление INNER JOIN с таблицей-справочником либо предком:
     * то есть должна найтись ровно 1 запись.
     * 
     * @param t Описание таблицы
     * @param names "AS псевдоним", список выбираемых полей
     * @return новый join
     */
    public JoinToOne toOne (Class t, String... names) {
        return toOne (table.getModel ().t (t), names);
    }
    
    /**
     * Добавление LEFT JOIN с таблицей-справочником либо предком:
     * то есть соответствующих записей может быть 0 или 1.
     * 
     * @param t Описание таблицы
     * @param names "AS псевдоним", список выбираемых полей
     * @return новый join
     */
    public JoinToOne toMaybeOne (Table t, String... names) {
        JoinToOne j = toOne (t, names);
        j.or0 ();
        return j;
    }
    
    /**
     * Добавление LEFT JOIN с таблицей-справочником либо предком:
     * то есть соответствующих записей может быть 0 или 1.
     * 
     * @param t Описание таблицы
     * @param names "AS псевдоним", список выбираемых полей
     * @return новый join
     */
    public JoinToOne toMaybeOne (String t, String... names) {
        return toMaybeOne (table.getModel ().t (t), names);
    }
    
    /**
     * Добавление LEFT JOIN с таблицей-справочником либо предком:
     * то есть соответствующих записей может быть 0 или 1.
     * 
     * @param t Описание таблицы
     * @param names "AS псевдоним", список выбираемых полей
     * @return новый join
     */
    public JoinToOne toMaybeOne (Class t, String... names) {
        return toMaybeOne (table.getModel ().t (t), names);
    }

    /**
     * Добавление LEFT JOIN с таблицей дочерних записей:
     * то есть может найтись произвольное число записей.
     * 
     * @param t Описание таблицы
     * @param names "AS псевдоним", список выбираемых полей
     * @return новый join
     */
    public JoinToMany toSome (Table t, String... names) {
        JoinToMany j = toSeveral (t, names);
        j.or0 ();
        return j;
    }
    
    /**
     * Добавление LEFT JOIN с таблицей дочерних записей:
     * то есть может найтись произвольное число записей.
     * 
     * @param t Описание таблицы
     * @param names "AS псевдоним", список выбираемых полей
     * @return новый join
     */
    public JoinToMany toSome (String t, String... names) {
        return toSome (table.getModel ().t (t), names);
    }
    
    /**
     * Добавление LEFT JOIN с таблицей дочерних записей:
     * то есть может найтись произвольное число записей.
     * 
     * @param t Описание таблицы
     * @param names "AS псевдоним", список выбираемых полей
     * @return новый join
     */
    public JoinToMany toSome (Class t, String... names) {
        return toSome (table.getModel ().t (t), names);
    }
    
}