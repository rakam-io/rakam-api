package org.rakam.analysis.query.simple.predicate;

import org.rakam.model.Entry;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/09/14 11:01.
 */
abstract class AbstractRichPredicate<T extends Entry> implements RichPredicate<T> {
    protected String attribute;

    public AbstractRichPredicate(String attribute) {
        this.attribute = attribute;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractRichPredicate)) return false;

        AbstractRichPredicate that = (AbstractRichPredicate) o;

        if (!attribute.equals(that.attribute)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return attribute.hashCode();
    }
}
