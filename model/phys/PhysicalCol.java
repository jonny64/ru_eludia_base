package ru.eludia.base.model.phys;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.json.Json;
import javax.json.JsonObjectBuilder;
import ru.eludia.base.model.abs.AbstractCol;

public final class PhysicalCol extends AbstractCol {
    
    JDBCType type;    
    String def;
    String ref = null;
    String fk = null;
    boolean virtual = false;

    public PhysicalCol (ResultSet rs) throws SQLException {
                
        super (rs.getString ("COLUMN_NAME"), rs.getInt ("COLUMN_SIZE"), rs.getInt ("DECIMAL_DIGITS"), rs.getString ("REMARKS"));

        type     = JDBCType.valueOf (rs.getInt ("DATA_TYPE"));
        nullable = rs.getInt    ("NULLABLE") == 1;
        def      = rs.getString ("COLUMN_DEF");

        if ("NULL".equals (def) || "NULL ".equals (def)) def = null;

    }

    public void setVirtual (boolean virtual) {
        this.virtual = virtual;
    }

    public void setFk (String fk) {
        this.fk = fk;
    }

    public String getRef () {
        return ref;
    }

    public String getFk () {
        return fk;
    }

    public boolean isRef () {
        return ref != null;
    }
    
    public boolean isVirtual () {
        return virtual;
    }

    public String getDef () {
        return def;
    }

    public JDBCType getType () {
        return type;
    }

    public PhysicalCol (JDBCType type, String name, int length, int precision) {
        super (name, length, precision);
        this.type = type;
    }
    
    public PhysicalCol (JDBCType type, String name, int length) {
        this (type, name, length, 0);
    }

    public PhysicalCol (JDBCType type, String name) {
        this (type, name, 0);
    }

    public void setDef (String def) {
        this.def = def;
    }    
    
    public void setRef (String ref) {
        this.ref = ref;
    }    

    @Override
    public String toString () {
        
        StringBuilder sb = new StringBuilder (type.name ());
        
        if (length > 0) {
            sb.append ('[');
            sb.append (length);
            if (precision > 0) {
                sb.append (',');
                sb.append (precision);
            }
            
            sb.append (']');            
        }
        
        final JsonObjectBuilder job = Json.createObjectBuilder ()
            .add ("name", name)
            .add ("type", sb.toString ());
        
        if (def == null) job.addNull ("def"); else job.add ("def", def);

        return job           
            .add ("nullable", nullable)
            .add ("virtual", virtual)
            .add ("remark", remark)
        .build ().toString ();
        
    }
    
}