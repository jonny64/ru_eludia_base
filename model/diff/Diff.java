package ru.eludia.base.model.diff;

import java.sql.JDBCType;
import java.util.logging.Level;
import java.util.logging.Logger;
import ru.eludia.base.DB;
import ru.eludia.base.model.phys.PhysicalCol;

public final class Diff {
    
    NullAction nullAction;
    TypeAction typeAction;
    boolean isCommentChanged;
    boolean isLengthChanged;
    
    private static final Logger logger = Logger.getLogger (Diff.class.getName ());    
    
    private static boolean isNumShorter (PhysicalCol asIs, PhysicalCol toBe) {
        return 
            (asIs.getLength    () < toBe.getLength    ()) || 
            (asIs.getPrecision () < toBe.getPrecision ()) ;        
    }

    private static boolean isShorter (PhysicalCol asIs, PhysicalCol toBe) {
        if (asIs.getType () == JDBCType.CLOB && toBe.getType () == JDBCType.VARCHAR) return false;
        if (asIs.getType () == JDBCType.NUMERIC && toBe.getType () == JDBCType.NUMERIC) return isNumShorter (asIs, toBe);
        return asIs.getLength () < toBe.getLength ();        
    }
    
    public Diff (PhysicalCol asIs, PhysicalCol toBe, DB db) {
        
        isCommentChanged = !asIs.getRemark ().equals (toBe.getRemark ());

        nullAction = NullAction.get (asIs.isNullable (), toBe.isNullable ());
                
        typeAction = db.getTypeAction (asIs.getType (), toBe.getType ());
        
        if (typeAction == null && isShorter (asIs, toBe)) typeAction = TypeAction.ALTER;
                
        if (typeAction == null && !db.equalDef (asIs, toBe)) {

            logger.info (toBe.getName () + ": DEFAULT changed from " + asIs.getDef () + " to " + toBe.getDef ());

            if (toBe.isVirtual ()) {
                typeAction = TypeAction.RECREATE;
            }
            else {                
                typeAction = TypeAction.ALTER;
            }

        }

        logger.log (Level.FINE, toBe + " - " + asIs + " = " + this);
        
    }

    public boolean isCommentChanged () {
        return isCommentChanged;
    }

    public TypeAction getTypeAction () {
        return typeAction;
    }

    public NullAction getNullAction () {
        return nullAction;
    }

    @Override
    public String toString () {
        return "{Diff: {typeAction:" + typeAction + ", nullAction:" + nullAction + ", isCommentChanged:" + isCommentChanged + "}}";
    }

}