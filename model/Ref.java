package ru.eludia.base.model;

import java.util.List;

public class Ref extends Col {
    
    Class c;
    Table targetTable;
    Col targetCol;
    
    public Ref (Object name, Class t, Object... p) {
        super (name, null, p);
        this.c = t;
    }

    @Override
    public Col clone () {
        return new Ref (name, c, def, remark); 
    }
    
    @Override
    public void setTable (Table table) {
        
        super.setTable (table);
        
        targetTable = table.getModel ().get (c);

        if (targetTable == null) throw new IllegalArgumentException ("Table not found for class " + c.getName ());
        
        List<Col> targetPk = targetTable.getPk ();
        
        if (targetPk.size () != 1) throw new IllegalArgumentException ("References to tables with vector PKs are not yet supported");
        
        targetCol = targetPk.get (0);
        
        this.type   = targetCol.getType ();
        this.length = targetCol.getLength ();
        
    }

    public Table getTargetTable () {
        return targetTable;
    }

    public Col getTargetCol () {
        return targetCol;
    }    
    
}
