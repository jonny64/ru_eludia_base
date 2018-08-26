package ru.eludia.base.model.def;

import java.math.BigDecimal;

public class Num extends Const {
    
    BigDecimal value;
    
    public static final Num ZERO = new Num (BigDecimal.ZERO);
    public static final Num ONE  = new Num (BigDecimal.ONE);

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