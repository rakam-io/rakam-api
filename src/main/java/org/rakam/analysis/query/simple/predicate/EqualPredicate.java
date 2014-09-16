package org.rakam.analysis.query.simple.predicate;

import org.rakam.model.Entry;

import java.util.function.Predicate;

/**
* Created by buremba <Burak Emre Kabakcı> on 15/09/14 13:54.
*/
public class EqualPredicate<T extends Entry> extends AbstractRichPredicate<T> {

    protected final Object value;

    public EqualPredicate(String attribute, Object value) {
        super(attribute);
        this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof EqualPredicate) {
            EqualPredicate p = (EqualPredicate) obj;
            return p.attribute.equals(attribute) && value.equals(p.value);
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
        final Number entryValue = entry.getAttribute(attribute);
        if (entryValue == null) {
            return false;
        }
        return entryValue.equals(value);
    }

    @Override
    public boolean isSubSet(Predicate predicate) {
        return equals(predicate);
    }
}
