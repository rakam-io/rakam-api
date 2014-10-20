package org.rakam.analysis.query.simple.predicate;

import org.vertx.java.core.json.JsonObject;

import java.util.function.Predicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/09/14 22:29.
 */
public interface ConnectorPredicate extends RichPredicate {
    Predicate<JsonObject> subtract(RichPredicate predicate);
    int getPredicateCount();
    Predicate<JsonObject>[] getPredicates();
    boolean contains(RichPredicate predicate);
}
