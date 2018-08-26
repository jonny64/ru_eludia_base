package ru.eludia.base.model;

import java.util.List;
import ru.eludia.base.model.def.Def;

public class Ref extends Col {
    
    Class c;
    Table targetTable;
    Col targetCol;
    
    public Ref (String name, Class t, Def def, String remark) {
        super (name, null, def, remark);
        this.c = t;
    }    
    
    public Ref (String name, Class t, String remark) {
        super (name, null, remark);
        this.c = t;
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
