package org.rakam.analysis.query.simple.predicate;

import org.rakam.model.Entry;
import org.rakam.util.NotImplementedException;
import org.vertx.java.core.json.JsonElement;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/09/14 22:23.
 */
public interface RichPredicate<T extends Entry> extends Predicate<T> {
    default boolean isSubSet(Predicate<T> predicate) {
        return false;
    }
    default JsonElement toJson() { throw new NotImplementedException(); }

    default Predicate<T> and(Predicate<? super T> other) {
        return new AndPredicate(this, Objects.requireNonNull(other));
    }

    default Predicate<T> or(Predicate<? super T> other) {
        return new OrPredicate(this, Objects.requireNonNull(other));
    }
}
