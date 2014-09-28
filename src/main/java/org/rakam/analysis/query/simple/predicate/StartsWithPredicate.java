package org.rakam.analysis.query.simple.predicate;

import org.rakam.model.Entry;
import org.rakam.util.ValidationUtil;
import org.vertx.java.core.json.JsonArray;

import java.util.function.Predicate;

/**
* Created by buremba <Burak Emre Kabakcı> on 15/09/14 13:54.
*/
public class StartsWithPredicate<T extends Entry> extends AbstractRichPredicate<T> {

    protected final String value;

    public StartsWithPredicate(String attribute, String str) {
        super(attribute);
        this.value = str;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof StartsWithPredicate) {
            StartsWithPredicate p = (StartsWithPredicate) obj;
            return p.attribute.equals(attribute) && value.startsWith(p.value);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public boolean test(T entry) {
        final String entryValue = entry.getAttribute(attribute);
        if (entryValue == null) {
            return false;
        }
        return entryValue.startsWith(value);
    }

    @Override
    public boolean isSubSet(Predicate predicate) {
        if (predicate instanceof StartsWithPredicate) {
            StartsWithPredicate p = (StartsWithPredicate) predicate;
            return ValidationUtil.equalOrNull(p.attribute, attribute) && p.value.startsWith(value);

        }
        return false;
    }

    @Override
    public JsonArray toJson() {
        return new JsonArray().addString(attribute).addString("$starts_with").addString(value);
    }
}
