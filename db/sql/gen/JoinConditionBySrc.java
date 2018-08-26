package ru.eludia.base.db.sql.gen;

import ru.eludia.base.db.sql.build.QP;

public final class JoinConditionBySrc extends JoinCondition {
    
    String src;

    public JoinConditionBySrc (String src) {
        this.src = src;
    }
    
    @Override
    public void add (QP qp) {        
        qp.append (src);
    }
        
}