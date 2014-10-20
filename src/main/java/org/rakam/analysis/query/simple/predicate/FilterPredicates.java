package org.rakam.analysis.query.simple.predicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 13/09/14 13:37.
 */
public class FilterPredicates {
    public static RichPredicate eq(String attr, Object o) {
        return new EqualPredicate(attr, o);
    }
    public static RichPredicate ne(String attr, Object o) {
        return new NegatePredicate(eq(attr, o));
    }

    public static RichPredicate contains(String attr) {
        return new ContainsPredicate(attr);
    }

    public static RichPredicate in(String attr, Object... o) {
        return new InPredicate(attr, o);
    }

    public static RichPredicate starts_with(String attr, String str) {
        return new StartsWithPredicate(attr, str);
    }

    public static RichPredicate regex(String attr, String regex) {
        return new RegexPredicate(attr, regex);
    }

    public static RichPredicate lt(String attr, long l) {
        return new LessPredicate(attr, l);
    }

    public static RichPredicate lte(String attr, long l) {
        return new LessEqualPredicate(attr, l);
    }

    public static RichPredicate gt(String attr, long l) {
        return new GreaterPredicate(attr, l);
    }

    public static RichPredicate gte(String attr, long l) {
        return new GreaterEqualPredicate(attr, l);
    }

}
