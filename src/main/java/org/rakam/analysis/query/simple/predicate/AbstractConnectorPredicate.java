package org.rakam.analysis.query.simple.predicate;

import org.rakam.util.json.JsonObject;

import java.util.function.Predicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/09/14 22:36.
 */
abstract class AbstractConnectorPredicate implements ConnectorPredicate {
    Predicate<JsonObject>[] predicates;

    public AbstractConnectorPredicate(Predicate predicate1, Predicate predicate2, Predicate... predicates) {
        if (predicates.length > 0) {
            this.predicates = new RichPredicate[predicates.length + 2];
            this.predicates[0] = predicate1;
            this.predicates[1] = predicate2;
            System.arraycopy(predicates, 0, this.predicates, 2, predicates.length);
        } else {
            this.predicates = new Predicate[]{predicate1, predicate2};
        }
    }

    @Override
    public Predicate<JsonObject>[] getPredicates() {
        return predicates;
    }

    @Override
    public int getPredicateCount() {
        int i = 0;
        for (Predicate predicate : predicates) {
            if (predicate instanceof ConnectorPredicate) {
                i += ((ConnectorPredicate) predicate).getPredicateCount();
            } else {
                i++;
            }
        }
        return i;
    }

    public String toStringInternal(String parameterName) {
        final StringBuilder sb = new StringBuilder();
        sb.append("(");
        int size = predicates.length;
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                sb.append(" " + parameterName + " ");
            }
            sb.append(predicates[i]);
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean isSubSet(Predicate predicate) {
        for (Predicate p : predicates) {
            if (p instanceof RichPredicate && ((RichPredicate) p).isSubSet(predicate)) {
                return true;
            }
        }
        return false;
    }
}
