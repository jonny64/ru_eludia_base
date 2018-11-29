package ru.eludia.base.model.def;

import java.math.BigDecimal;
import java.math.BigInteger;

public class Num extends Const {
    
    BigDecimal value;
    
    public static final Num ZERO = new Num (BigDecimal.ZERO);
    public static final Num ONE  = new Num (BigDecimal.ONE);
    public static final Num TEN  = new Num (BigDecimal.TEN);
    
    public static Num valueOf (BigDecimal n) {
        if (BigDecimal.ZERO.equals (n)) return ZERO;
        if (BigDecimal.ONE.equals (n)) return ONE;
        if (BigDecimal.TEN.equals (n)) return TEN;
        throw new IllegalArgumentException ("Invalid BigDecimal DEFAULT value: " + n);
    }
    
    public static Num valueOf (BigInteger n) {
        if (BigInteger.ZERO.equals (n)) return ZERO;
        if (BigInteger.ONE.equals (n)) return ONE;
        if (BigInteger.TEN.equals (n)) return TEN;
        throw new IllegalArgumentException ("Invalid BigInteger DEFAULT value: " + n);
    }

    public Num (BigDecimal value) {
        this.value = value;
    }

    public Num (int value) {
        this.value = BigDecimal.valueOf (value);
    }
    
    public Num (long value) {
        this.value = BigDecimal.valueOf (value);
    }

    @Override
    public java.lang.String toSql () {
        return value.toPlainString ();
    }

    @Override
    public Object getValue () {
        return value;
    }

}