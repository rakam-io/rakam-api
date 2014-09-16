package org.rakam.analysis.query.simple.predicate;

import org.rakam.model.Entry;

import java.util.function.Predicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 13/09/14 13:37.
 */
public class FilterPredicates {
    public static RichPredicate<Entry> eq(String attr, Object o) {
        return new EqualPredicate<Entry>(attr, o);
    }
    public static RichPredicate<Entry> ne(String attr, Object o) {
        return new NegatePredicate<Entry>(eq(attr, o));
    }

    public static Predicate<Entry> contains(String attr) {
        return new ContainsPredicate<Entry>(attr);
    }

    public static Predicate<Entry> in(String attr, Object... o) {
        return new InPredicate<Entry>(attr, o);
    }

    public static RichPredicate<Entry> starts_with(String attr, String str) {
        return new StartsWithPredicate<Entry>(attr, str);
    }

    public static RichPredicate<Entry> regex(String attr, String regex) {
        return new RegexPredicate<Entry>(attr, regex);
    }

    public static RichPredicate<Entry> lt(String attr, long l) {
        return new LessPredicate<Entry>(attr, l);
    }

    public static RichPredicate<Entry> lte(String attr, long l) {
        return new LessEqualPredicate<Entry>(attr, l);
    }

    public static RichPredicate<Entry> gt(String attr, long l) {
        return new GreaterPredicate<Entry>(attr, l);
    }

    public static RichPredicate<Entry> gte(String attr, long l) {
        return new GreaterEqualPredicate<Entry>(attr, l);
    }

}
