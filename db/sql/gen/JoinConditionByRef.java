package ru.eludia.base.db.sql.gen;

import ru.eludia.base.db.sql.build.QP;

public final class JoinConditionByRef extends JoinCondition {
    
    PartRef from;
    Part toPart;
    
    public JoinConditionByRef (PartRef from, Part toPart) {
        this.from = from;
        this.toPart = toPart;
    }
    
    @Override
    public void add (QP qp) {        
        qp.append (from.getPart ().getTableAlias ());
        qp.append ('.');
        qp.append (from.getRef ().getName ());
        qp.append ('=');
        qp.append (toPart.getTableAlias ());
        qp.append ('.');
        qp.append (from.getRef ().getTargetCol ().getName ());
    }
        
}