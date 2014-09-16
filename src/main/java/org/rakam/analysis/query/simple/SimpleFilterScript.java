package org.rakam.analysis.query.simple;

import org.rakam.analysis.query.FilterScript;
import org.vertx.java.core.json.JsonObject;

import java.util.function.Predicate;

/**
 * Created by buremba on 05/05/14.
 */
public class SimpleFilterScript extends FilterScript {
    final boolean requiresUser;
    public Predicate predicate;

    public SimpleFilterScript(Predicate predicate, boolean requiresUser) {
        this.predicate = predicate;
        this.requiresUser = requiresUser;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleFilterScript)) return false;

        SimpleFilterScript that = (SimpleFilterScript) o;

        if (requiresUser != that.requiresUser) return false;
        if (!predicate.equals(that.predicate)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (requiresUser ? 1 : 0);
        result = 31 * result + predicate.hashCode();
        return result;
    }

    @Override
    public boolean test(JsonObject obj) {
        return predicate.test(obj);
    }

    @Override
    public boolean requiresUser() {
        return requiresUser;
    }
}
