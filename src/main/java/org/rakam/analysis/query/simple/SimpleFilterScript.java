package org.rakam.analysis.query.simple;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.query.simple.predicate.RichPredicate;
import org.rakam.util.json.JsonElement;

import java.util.function.Predicate;

/**
 * Created by buremba on 05/05/14.
 */
public class SimpleFilterScript implements FilterScript {
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
    public boolean test(ObjectNode obj) {
        return predicate.test(obj);
    }

    @Override
    public boolean requiresUser() {
        return requiresUser;
    }

    @Override
    public JsonElement toJson() {
        if (predicate instanceof RichPredicate) {
            return ((RichPredicate) predicate).toJson();
        }
        return JsonElement.valueOf(predicate.toString());
    }
}
