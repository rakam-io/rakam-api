package org.rakam.analysis.query.simple.predicate;

import org.rakam.util.json.JsonArray;
import org.rakam.util.json.JsonObject;

import java.util.function.Predicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/09/14 13:54.
 */
public class RegexPredicate extends AbstractRichPredicate {

    protected final String regex;

    public RegexPredicate(String attribute, String regex) {
        super(attribute);
        this.regex = regex;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RegexPredicate) {
            RegexPredicate r = (RegexPredicate) obj;
            return r.attribute.equals(attribute) && r.regex.equals(regex);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + regex.hashCode();
        return result;
    }

    @Override
    public boolean test(JsonObject entry) {
        final String entryValue = entry.getString(attribute);
        if (entryValue == null) {
            return false;
        }
        return entryValue.matches(regex);
    }

    @Override
    public boolean isSubSet(Predicate predicate) {
        return equals(predicate);
    }

    @Override
    public JsonArray toJson() {
        return new JsonArray().add(attribute).add("$regex").add(regex);
    }
}
