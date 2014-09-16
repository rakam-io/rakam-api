package org.rakam.analysis.query.simple.predicate;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/09/14 00:06.
 */
public class OrPredicate extends AbstractConnectorPredicate {

    public OrPredicate(Predicate predicate1, Predicate predicate2, Predicate... predicates) {
        super(predicate1, predicate2, predicates);
    }


    @Deprecated
    public boolean equals(Object predicate) {
        if (predicate instanceof OrPredicate) {
            OrPredicate orPredicate = ((OrPredicate) predicate);
            outer:
            for (Predicate predicate1 : predicates) {
                for (Predicate predicate2 : orPredicate.getPredicates()) {
                    if (predicate1.equals(predicate2)) {
                        continue outer;
                    }
                }
                return false;
            }
            return orPredicate.getPredicateCount() == getPredicateCount();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(predicates);
    }

    @Override
    public String toString() {
        return toStringInternal("OR");
    }


    @Override
    public boolean test(Object mapEntry) {
        for (Predicate predicate : predicates) {
            if (predicate.test(mapEntry)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RichPredicate subtract(RichPredicate predicate) {
        if (!contains(predicate)) {
            throw new IllegalArgumentException("this.predicate must contain predicate");
        }

        List<RichPredicate> listPredicate = new LinkedList(Arrays.asList(predicates));
        if (predicate instanceof ConnectorPredicate) {
            for (Predicate p : ((ConnectorPredicate) predicate).getPredicates()) {
                listPredicate.remove(p);
            }
        } else {
            listPredicate.remove(predicate);
        }

        if (listPredicate.size() == 1) {
            return listPredicate.get(0);
        }

        if (listPredicate.size() == 0) {
            return null;
        }
        if (listPredicate.size() == 1) {
            return listPredicate.get(0);
        }

        RichPredicate firstPredicate = listPredicate.remove(0);
        RichPredicate secondPredicate = listPredicate.remove(0);
        return new OrPredicate(firstPredicate, secondPredicate, listPredicate.toArray(new RichPredicate[listPredicate.size()]));
    }

    @Override
    public boolean contains(RichPredicate predicate) {
        if (predicate instanceof OrPredicate) {
            for (Predicate pInline : ((ConnectorPredicate) predicate).getPredicates()) {
                if(pInline instanceof RichPredicate)
                if (!(pInline instanceof RichPredicate) || !this.contains(((RichPredicate) pInline))) {
                    return false;
                }
            }

            return true;
        } else {
            for (Predicate p : predicates) {
                if (p instanceof OrPredicate) {
                    if (((ConnectorPredicate) p).contains(predicate)) {
                        return true;
                    }
                } else if (predicate.equals(p)) {
                    return true;
                }
            }
        }
        return false;
    }

}
