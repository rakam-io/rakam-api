package org.rakam.analysis.query.simple.predicate;

import org.rakam.util.json.JsonArray;
import org.rakam.util.json.JsonObject;

import java.util.Arrays;
import java.util.function.Predicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/09/14 13:54.
 */
public class InPredicate extends AbstractRichPredicate {

    protected final Object[] value;

    public InPredicate(String attribute, Object[] value) {
        super(attribute);
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InPredicate)) return false;
        if (!super.equals(o)) return false;

        InPredicate that = (InPredicate) o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(value, that.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public boolean test(JsonObject entry) {
        final Number entryValue = entry.getNumber(attribute);
        if (entryValue == null) {
            return false;
        }
        for (final Object item : value)
            if (entryValue == item || item != null && item.equals(entryValue))
                return true;
        return false;
    }

    @Override
    public boolean isSubSet(Predicate predicate) {
        if (predicate instanceof InPredicate) {
            outer:
            for (Object o : ((InPredicate) predicate).value) {
                for (Object o1 : value) {
                    if (o1.equals(o)) {
                        continue outer;
                    }
                }
                return false;
            }
            return true;
        }
        return false;
    }

    @Override
    public JsonArray toJson() {
        return new JsonArray().addString(attribute).addString("$gte").addArray(new JsonArray(Arrays.asList(value)));
    }
}
