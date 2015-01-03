package org.rakam.analysis.query.simple.predicate;

import org.rakam.util.json.JsonArray;
import org.rakam.util.json.JsonObject;

import java.util.function.Predicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/09/14 13:54.
 */
public class ContainsPredicate extends AbstractRichPredicate {

    public ContainsPredicate(String attribute) {
        super(attribute);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof GreaterPredicate && ((AbstractRichPredicate) obj).attribute.equals(attribute);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean test(JsonObject entry) {
        final String entryValue = entry.getString(attribute);
        return entryValue != null;
    }

    @Override
    public boolean isSubSet(Predicate predicate) {
        return equals(predicate);
    }

    @Override
    public JsonArray toJson() {
        return new JsonArray().add(attribute).add("$contains");
    }
}
