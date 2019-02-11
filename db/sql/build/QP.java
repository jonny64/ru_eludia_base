package ru.eludia.base.db.sql.build;

import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import ru.eludia.base.model.phys.PhysicalCol;
import ru.eludia.base.db.util.ParamSetter;

/**
 * Вариант SQLBuilder'а для сборки SQL напрямую в коде приложения
 * (а не автоматом по описанию таблицы).
 */
public class QP extends SQLBuilder {
    
    List <Object> params = new ArrayList <> ();

    /**
     * Добавить вложенный фрагмент SQL вместе со списком параметров.
     * @param qp что нужно вложить.
     */
    public void add (QP qp) {
        sb.append (qp.sb);
        if (qp.cols == null) return;
        if (cols == null) cols = qp.cols; else cols.addAll (qp.cols);
        params.addAll (qp.params);
        psb.append (qp.psb);
    }
    
    public QP (String sql, List<Object> p) {
        super (sql);
        params.addAll (p);
    }

    public QP (String sql, Object... p) {
        this (sql, Arrays.asList (p));
    }
            
    /**
     * Приписка к SQL имени поля 
     * с одновременным запоминанием значения соответствующего параметра.
     * @param col имя поля таблицы
     * @param param значение параметра
     */
    public final void add (PhysicalCol col, Object param) {
        add (col.getName (), param, col);        
    }

    /**
     * Приписка к SQL фрагмента кода
     * с одновременным запоминанием типа и значения соответствующего параметра.
     * @param sql фрагмент SQL
     * @param param значение параметра
     * @param col описание поля таблицы
     */
    public final void add (String sql, Object param, PhysicalCol col) {        
        append (sql, col);                        
        params.add (param);        
    }

    /**
     * Индикатор того, что для данного SQL требуется prepareStatament.
     * @return true, если список параметров непуст (и их потребуется 
     * передавать после prepareStatement) и false в противном случае 
     * (когда SQL должен быть исполнен посредством createStatement)
     */
    @Override
    public boolean isToPrepare () {
        return !params.isEmpty ();
    }

    /**
     * Накопленный список параметров
     * @return он самый
     */
    public List<Object> getParams () {
        return params;
    }
            
    void setParams (PreparedStatement st, ParamSetter ps, List<Object> params, ParameterMetaData md) throws SQLException {

        for (int n = 1; n <= md.getParameterCount (); n ++) {
            
            Object p = params.get (n - 1);

            ps.setParam (st, n, JDBCType.valueOf (md.getParameterType (n)), md.getPrecision (n), p);
            
            logParam (p);

        }

    }

    void setParams (PreparedStatement st, ParamSetter ps, List<Object> params, List <PhysicalCol> cols) throws SQLException {
        
        openLogRecord ();

        for (int n = 1; n <= cols.size (); n ++) {
            
            Object p = params.get (n - 1);
            
            final PhysicalCol col = cols.get (n - 1);
            
            ps.setParam (st, n, col.getType (), col.getLength (), p);
            
            logParam (p);

        }

        closeLogRecord ();
        
    }
    
    @Override
    public void setParams (PreparedStatement st, ParamSetter ps) throws SQLException {

        if (params.isEmpty ()) return;
                    
        if (cols != null) 
            
            setParams (st, ps, params, cols); 
        
        else
            
            setParams (st, ps, params, st.getParameterMetaData ());
                    
    }    

}