package ru.eludia.base.db.util;

import java.util.HashMap;
import java.util.function.BiConsumer;

public class BiConsumers<T, U> extends HashMap<String, BiConsumer<T, U>> {
    
    public final BiConsumers<T, U> set (String s, BiConsumer<T, U> b) {
        put (s, b);
        return this;
    }
    
}
