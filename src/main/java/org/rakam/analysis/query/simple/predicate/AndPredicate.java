package org.rakam.analysis.query.simple.predicate;

import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/09/14 22:32.
 */
public class AndPredicate extends AbstractConnectorPredicate {

    public AndPredicate(Predicate predicate1, Predicate predicate2, Predicate... predicates) {
        super(predicate1, predicate2, predicates);
    }

    @Override
    public boolean equals(Object predicate) {
        if (predicate instanceof AndPredicate) {
            ConnectorPredicate p = (ConnectorPredicate) predicate;
            return this.contains(p) && p.contains(this);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(predicates);
    }

    @Override
    public String toString() {
        return toStringInternal("AND");
    }

    @Override
    public RichPredicate subtract(RichPredicate predicate) {
        if (!contains(predicate)) {
            throw new IllegalArgumentException("this.predicate must contain predicate");
        }

        List<RichPredicate> listPredicate = new LinkedList(Arrays.asList(predicates));
        if (!listPredicate.remove(predicate)) {
            if (predicate instanceof ConnectorPredicate) {
                for (Predicate p : ((ConnectorPredicate) predicate).getPredicates()) {
                    listPredicate.remove(p);
                }
            } else {
                listPredicate.remove(predicate);
            }
        }
        if (listPredicate.size() == 0) {
            return null;
        }
        if (listPredicate.size() == 1) {
            return listPredicate.get(0);
        }

        RichPredicate firstPredicate = listPredicate.remove(0);
        RichPredicate secondPredicate = listPredicate.remove(0);

        return new AndPredicate(firstPredicate, secondPredicate, listPredicate.toArray(new RichPredicate[listPredicate.size()]));
    }

    @Override
    public boolean test(Object mapEntry) {
        for (Predicate predicate : predicates) {
            if (!predicate.test(mapEntry)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean contains(RichPredicate predicate) {
        if (predicate instanceof AndPredicate) {
            for (Predicate pInline : ((ConnectorPredicate) predicate).getPredicates()) {
                if (pInline instanceof RichPredicate && !this.contains((RichPredicate) pInline)) {
                    return false;
                }
            }
            return true;

        } else {
            for (Predicate predicate1 : predicates) {
                if (predicate1.equals(predicate)) {
                    return true;
                } else if (predicate1 instanceof AndPredicate) {
                    if (((ConnectorPredicate) predicate1).contains(predicate)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    @Override
    public JsonObject toJson() {
        JsonArray objects = new JsonArray();
        for (Predicate predicate : predicates) {
            if(predicate instanceof RichPredicate)
                objects.add(((RichPredicate) predicate).toJson());
            else
                objects.add(predicate.toString());
        }
        return new JsonObject().putArray("AND", objects);
    }
}
