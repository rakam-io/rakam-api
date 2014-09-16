package org.rakam.analysis.query.simple.predicate;

import org.rakam.model.Entry;
import org.rakam.util.ValidationUtil;

import java.util.function.Predicate;

/**
* Created by buremba <Burak Emre Kabakcı> on 15/09/14 13:54.
*/
public class StringContainsPredicate<T extends Entry> extends AbstractRichPredicate<T> {

    protected final String value;

    public StringContainsPredicate(String attribute, String value) {
        super(attribute);
        this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof GreaterPredicate && ((AbstractRichPredicate) obj).attribute.equals(attribute);
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
        return entryValue.contains(value);
    }

    @Override
    public boolean isSubSet(Predicate predicate) {
        if (predicate instanceof StringContainsPredicate) {
            StringContainsPredicate p = (StringContainsPredicate) predicate;
            return ValidationUtil.equalOrNull(p.attribute, attribute) && p.value.contains(value);

        }
        return false;
    }
}
