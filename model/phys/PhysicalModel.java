package ru.eludia.base.model.phys;

import ru.eludia.base.model.Col;
import ru.eludia.base.model.View;
import ru.eludia.base.model.abs.AbstractModel;

public final class PhysicalModel extends AbstractModel<PhysicalCol, PhysicalKey, PhysicalTable> {
    
    public String getSql (View v) {
        
        PhysicalTable pt = tables.get (v.getName ());

        if (pt == null) return null;
        
        if (!(pt instanceof PhysicalView)) return null;

        return ((PhysicalView) pt).getSql ();
        
    }
    
    public String getRemark (Col col) {
        
        PhysicalTable pt = tables.get (col.getTable ().getName ());
        
        if (pt == null) return null;
        
        PhysicalCol pc = pt.getColumn (col.getName ());
        
        return pc == null ? null : pc.getRemark ();
        
    }
    
}