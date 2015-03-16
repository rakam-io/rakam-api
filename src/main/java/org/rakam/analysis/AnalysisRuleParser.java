package org.rakam.analysis;

import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.query.simple.SimpleFilterScript;
import org.rakam.analysis.query.simple.predicate.FilterPredicates;
import org.rakam.util.Tuple;
import org.rakam.util.json.JsonArray;
import org.rakam.util.json.JsonElement;
import org.rakam.util.json.JsonObject;

import java.util.Map;
import java.util.function.Predicate;

/**
 * Created by buremba on 15/01/14.
 */
public class AnalysisRuleParser {

    public static Tuple<Predicate, Boolean> generatePredicate(JsonElement from) {
        Predicate to = null;
        boolean requiresUser = false;

        if (from.isArray()) {
            for (Object item : (JsonArray) from) {
                if (item instanceof JsonArray) {
                    JsonArray item1 = (JsonArray) item;
                    String field = item1.getString(0);
                    if (field.startsWith("_user.")) {
                        requiresUser = true;
                    }
                    Predicate predicate = createPredicate(item1.getString(1), field, item1.getString(2));
                    to = to == null ? predicate : to.and(predicate);
                } else if (item instanceof JsonObject) {
                    Tuple<Predicate, Boolean> predicateBooleanTuple = generatePredicate((JsonElement) item);
                    Predicate predicate = predicateBooleanTuple.v1();
                    requiresUser = requiresUser || predicateBooleanTuple.v2();
                    to = to == null ? predicate : to.or(predicate);
                }
            }
        } else
        if(from.isObject()) {
            for (Map.Entry<String, Object> field : (JsonObject) from) {
                Tuple<Predicate, Boolean> predicateBooleanTuple = generatePredicate((JsonElement) field.getValue());
                Predicate predicate = predicateBooleanTuple.v1();
                requiresUser = requiresUser || predicateBooleanTuple.v2();
                switch (field.getKey()) {
                    case "AND":
                        to = to == null ? predicate : to.or(predicate);
                        break;
                    case "OR":
                        to = to == null ? predicate : to.or(predicate);
                        break;
                    default:
                        throw new IllegalArgumentException();
                }
            }
        }
        if (to == null)
            return null;
        return new Tuple(to, requiresUser);
    }

    private static Predicate createPredicate(String operator, String field, Object argument) {
        switch (operator) {
            case "$lte":
                return FilterPredicates.lte(field, ((Number) argument).longValue());
            case "$lt":
                return FilterPredicates.lt(field, ((Number) argument).longValue());
            case "$gt":
                return FilterPredicates.gt(field, ((Number) argument).longValue());
            case "$gte":
                return FilterPredicates.gte(field, ((Number) argument).longValue());
            case "$contains":
                return FilterPredicates.contains(field);
            case "$eq":
                return FilterPredicates.eq(field, argument);
            case "$in":
                return FilterPredicates.in(field, ((JsonArray) argument).stream().toArray());
            case "$ne":
                return FilterPredicates.ne(field, argument);
            case "$regex":
                return FilterPredicates.regex(field, (String) argument);
            case "$starts_with":
                return FilterPredicates.starts_with(field, (String) argument);
            default:
                throw new IllegalArgumentException("undefined operator " + operator);
        }
    }

    public static FilterScript getFilter(JsonElement field) {
        if (field == null) {
            return null;
        }
        if (field.isObject()) {
            String script = ((JsonObject) field).getString("script");
            if (script != null) {
//                return new MVELFilterScript(script);
            }
        }

        Tuple<Predicate, Boolean> predicateBooleanTuple = generatePredicate(field);
        if (predicateBooleanTuple == null)
            return null;
        return new SimpleFilterScript(predicateBooleanTuple.v1(), predicateBooleanTuple.v2());
    }
}
