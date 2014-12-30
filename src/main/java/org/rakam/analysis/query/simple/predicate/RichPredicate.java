package org.rakam.analysis.query.simple.predicate;

import org.rakam.util.NotImplementedException;
import org.rakam.util.json.JsonElement;
import org.rakam.util.json.JsonObject;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/09/14 22:23.
 */
public interface RichPredicate extends Predicate<JsonObject> {
    default boolean isSubSet(Predicate<JsonObject> predicate) {
        return false;
    }

    default JsonElement toJson() {
        throw new NotImplementedException();
    }

    default Predicate<JsonObject> and(Predicate<? super JsonObject> other) {
        return new AndPredicate(this, Objects.requireNonNull(other));
    }

    default Predicate<JsonObject> or(Predicate<? super JsonObject> other) {
        return new OrPredicate(this, Objects.requireNonNull(other));
    }
}
