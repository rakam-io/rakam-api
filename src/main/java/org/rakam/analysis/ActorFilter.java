package org.rakam.analysis;

import org.rakam.analysis.query.simple.SimpleFilterScript;
import org.rakam.cache.CacheAdapter;
import org.rakam.database.DatabaseAdapter;
import org.rakam.model.Actor;
import org.rakam.util.Tuple;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.function.Predicate;

import static org.rakam.analysis.AnalysisRuleParser.generatePredicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 03/09/14 15:51.
 */
public class ActorFilter {
    private final DatabaseAdapter databaseAdapter;
    private final CacheAdapter cacheAdapter;

    public ActorFilter(CacheAdapter cacheAdapter, DatabaseAdapter databaseAdapter) {
        this.databaseAdapter = databaseAdapter;
        this.cacheAdapter = cacheAdapter;
    }

    public JsonObject handle(JsonObject query) {
        Integer limit = query.getInteger("limit");
        String orderBy = query.getString("orderBy");
        JsonObject filterJson = query.getObject("filter");
        if (filterJson != null) {
            return new JsonObject().putString("error", "filter parameter is required");
        }
        Tuple<Predicate, Boolean> predicateBooleanTuple;
        try {
            predicateBooleanTuple = generatePredicate(filterJson);
        } catch (Exception e) {
            return new JsonObject().putString("error", "couldn't parse filter parameter");
        }
        SimpleFilterScript filter = new SimpleFilterScript(predicateBooleanTuple.v1(), predicateBooleanTuple.v2());
        Actor[] actors = databaseAdapter.filterActors(filter, limit, orderBy);
        return new JsonObject().putArray("result", new JsonArray(actors));
    }
}
