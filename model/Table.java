package ru.eludia.base.model;

import ru.eludia.base.Model;
import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import ru.eludia.base.model.abs.AbstractTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.json.JsonObject;
import ru.eludia.base.DB;
import ru.eludia.base.db.dialect.Oracle;
import ru.eludia.base.model.def.Def;

public abstract class Table extends AbstractTable<Col, Key> {
    
    protected Model model;
    protected Oracle.TemporalityType temporalityType;
    protected Oracle.TemporalityRowsAction temporalityRowsAction;

    public void setTemporality (Oracle.TemporalityType temporalityType, Oracle.TemporalityRowsAction temporalityRowsAction) {
        this.temporalityType = temporalityType;
        if (temporalityRowsAction == null) temporalityRowsAction = Oracle.TemporalityRowsAction.DELETE;
        this.temporalityRowsAction = temporalityRowsAction;
    }

    public Oracle.TemporalityType getTemporalityType () {
        return temporalityType;
    }

    public Oracle.TemporalityRowsAction getTemporalityRowsAction () {
        return temporalityRowsAction;
    }

    public final void setModel (Model model) {
        this.model = model;
    }

    public final Model getModel () {
        return model;
    }
    
    List<Map <String, Object>> data = Collections.EMPTY_LIST;

    public Table (String name) {
        super (name);
    }
    
    public Table (String name, String remark) {
        super (name, remark);
    }
            
    protected final void pk (Object name, Type type, Object... p) {
        pk (new Col (name, type, p));
    }

    protected final void col (Object name, Type type, Object... p) {
        add (new Col (name, type, p));
    }
      
    protected final void pkref (String name, Class t, String remark) {
        pk (new Ref (name, t, remark));
    }
        
    protected final void fk (String name, Class t, Object... p) {
        add (new Ref (name, t, p));
    }

    protected final void key (String name, Object... parts) {
        add (new Key (name, parts));
    }
    
    protected final void key (ColEnum col) {
        String lc = col.lc ();
        key (lc, lc);
    }
    
    protected final void unique (String name, Object... parts) {
        Key key = new Key (name, parts);
        key.setUnique (true);
        add (key);
    }

    protected final void ref (String name, Class t, Object... p) {
        add (new Ref (name, t, p));
        add (new Key (name, name));        
    }
        
    protected final void item (Object... o) {
        if (data.isEmpty ()) data = new ArrayList<> (1);
        data.add (DB.HASH (o));
    }
    
    protected final void trigger (String when, String what) {
        Trigger trg = new Trigger (when, what);
        triggers.add (trg);
    }
    
    protected final void cols (Class clazz) {                
        for (Object value: clazz.getEnumConstants ()) add (((ColEnum) value).getCol ().clone ());
    }

    protected final void data (Class clazz) {
        
        Object [] values = clazz.getEnumConstants ();
        
        if (values.length == 0) return;
        
        if (data.isEmpty ()) data = new ArrayList<> (1);
        
        try {
            
            BeanInfo info = Introspector.getBeanInfo (values [0].getClass ());
            
            PropertyDescriptor [] props = info.getPropertyDescriptors();

            for (Object value: values) {
                
                Map <String, Object> i = DB.HASH ();
                
                for (PropertyDescriptor pd: props) {
                    
                    String name = pd.getName ();
                    
                    if ("class".equals (name) || "declaringClass".equals (name)) continue;
                    
                    i.put (name, pd.getReadMethod ().invoke (value));
                    
                }

                data.add (i);
            
            }
                        
        }
        catch (Exception ex) {
            throw new IllegalArgumentException (ex);
        }
                
    }

    public List<Map<String, Object>> getData () {
        return data;
    }
    
    /**
     * Определить имя ДРУГОЙ таблицы в той же модели.
     * @param table java-класс описания таблицы
     * @return имя таблицы в БД
     */
    protected final String getName (Class table) {
        return getModel ().t (table).getName ();
    }
    
    /**
     * Конструктор хэш-таблицы из JSON-объекта, с возможностью обязательного переопределения некоторых полей
     * @param data JSON-объект с набором полей, одноимённых столбцам данной таблицы
     * @param o список объектов, где через один идут ключи и соответствующие им значения
     * @return хэш из всего переданного
     */
    public Map<String, Object> HASH (JsonObject data, Object... o) {

        int len = o.length;

        Map <String, Object> m = new HashMap (columns.size () + (len >> 1));

        for (Col col: columns.values ()) {            

            final String colName = col.getName ();

            if (!data.containsKey (colName)) continue;

            m.put (colName, DB.to.object (data.get (colName)));
            
        }

        for (int i = 0; i < len; i += 2) m.put (DB.lc (o [i]), o [i + 1]);

        return m;
        
    }

}