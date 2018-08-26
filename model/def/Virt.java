package ru.eludia.base.model.def;

public class Virt extends Def {
    
    java.lang.String expression;

    public Virt (java.lang.String expression) {
        this.expression = expression;
    }

    public java.lang.String getExpression () {
        return expression;
    }

    @Override
    public Object getValue () {
        throw new IllegalStateException ("Virtual columns can not be used for parameter binding");
    }
        
}
