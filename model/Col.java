package ru.eludia.base.model;

import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import javax.json.Json;
import javax.json.JsonObjectBuilder;
import ru.eludia.base.model.abs.AbstractCol;
import ru.eludia.base.model.def.Def;
import ru.eludia.base.model.def.Null;
import ru.eludia.base.model.phys.PhysicalCol;

public class Col extends AbstractCol implements Cloneable {
    
    Type type;    
    Def def = null;
    Table table;
    PhysicalCol physicalCol;
    
    public void setTable (Table table) {
        this.table = table;
    }

    public Def getDef () {
        return def;
    }

    public void setDef (Def def) {
        this.def = def;
    }

    public Type getType () {
        return type;
    }

    public Col clone (String name) {
        Col c = clone ();
        c.name = name;
        return c;
    }
    
    @Override
    public Col clone () {

        Col clone;
        
        try {
            clone = (Col) super.clone ();
        }
        catch (CloneNotSupportedException ex) {
            throw new IllegalStateException ("Impossible", ex);
        }

        clone.name = name;
        clone.remark = remark;

        clone.type = type;
        clone.def = def;

        clone.length = length;
        clone.precision = precision;

        return clone;

    }
        
    public Col (Object name, Type type, Object... p) {
        
        super (name.toString ().toLowerCase (), p [p.length - 1].toString ());
        
        this.type = type;
        
        int len = p.length;
        
        if (len == 1) return;
        
        def = Def.valueOf (p [len - 2]);
        
        if (def instanceof Null) {
            def = null;
            nullable = true;
        }

        if (p [0] instanceof Integer) {
            length = (Integer) p [0];
            if (len == 2) return;
            if (p [1] instanceof Integer) precision = (Integer) p [1];
        }
                
    }
    
    public Table getTable () {
        return table;
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
        
        if (def == null) job.addNull ("def"); else job.add ("def", def.toString ());

        return job           
            .add ("nullable", nullable)
            .add ("remark", remark)
        .build ().toString ();
        
    }

    public PhysicalCol toPhysical () {
        return physicalCol;
    }

    public void setPhysicalCol (PhysicalCol physicalCol) {
        this.physicalCol = physicalCol;
    }
    
    public static Random random = new Random ();
    
    public Supplier<Object> getValueGenerator () {
        
        switch (type) {
            
            case BOOLEAN: return () -> {
                return random.nextBoolean ();
            };
            
            case INTEGER: return () -> {
                return BigInteger.valueOf (random.nextLong ()).mod (BigInteger.TEN.pow (physicalCol.getLength ())).abs ();
            };
            
            case MONEY:
            case NUMERIC: return () -> {
                final BigInteger bi = BigInteger.valueOf (random.nextLong ());
                final BigDecimal raw = new BigDecimal (bi, precision).abs ();
                if (raw.precision () - raw.scale () <= length - precision) return raw;
                BigDecimal [] divideAndRemainder = raw.divideAndRemainder (BigDecimal.TEN.pow (length - precision));
                return divideAndRemainder [1];
            };
            
            case UUID: return () -> {
                return UUID.randomUUID ();
            };
            
            case DATE: return () -> {
                Calendar cal = Calendar.getInstance ();
                cal.add (Calendar.DATE, random.nextInt (500));
                return new java.sql.Timestamp (cal.getTimeInMillis ());
            };
            
            case DATETIME:
            case TIMESTAMP: return () -> {
                return new java.sql.Timestamp (System.currentTimeMillis () + random.nextInt ());
            };
            
            case STRING: return () -> {
                return new String (new char [physicalCol.getLength ()]).replace ('\0', (char) ('A' + random.nextInt (60)));
            };
            
            case TEXT: return () -> {
                return new String (new char [8001]).replace ('\0', 's');
            };
            
            case BINARY: return () -> {
                return new byte [physicalCol.getLength ()];
            };
            
            case BLOB: return () -> {
                return "CAFEBABE";
            };
            
            default: return null;
            
        }
        
    }

}