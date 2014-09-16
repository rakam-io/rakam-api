package org.rakam.analysis.query.simple.predicate;

import org.rakam.model.Entry;

import java.util.function.Predicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/09/14 22:29.
 */
public interface ConnectorPredicate<T extends Entry> extends RichPredicate<T> {
    Predicate<T> subtract(RichPredicate<T> predicate);
    int getPredicateCount();
    Predicate<T>[] getPredicates();
    boolean contains(RichPredicate<T> predicate);
}
