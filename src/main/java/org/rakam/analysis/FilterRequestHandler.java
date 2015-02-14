package org.rakam.analysis;

import org.rakam.analysis.query.simple.SimpleFilterScript;
import org.rakam.server.RouteMatcher;
import org.rakam.server.http.HttpService;
import org.rakam.util.Tuple;
import org.rakam.util.json.JsonObject;

import java.util.function.Predicate;

import static org.rakam.analysis.AnalysisRuleParser.generatePredicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 03/09/14 02:12.
 */
public class FilterRequestHandler implements HttpService {


    public JsonObject filterEvents(JsonObject query) {
        Integer limit = query.getInteger("limit");
        String orderBy = query.getString("orderBy");
        JsonObject filterJson = query.getJsonObject("filter");
        if (filterJson != null) {
            return new JsonObject().put("error", "filter parameter is required");
        }
        Tuple<Predicate, Boolean> predicateBooleanTuple;
        try {
            predicateBooleanTuple = generatePredicate(filterJson);
        } catch (Exception e) {
            return new JsonObject().put("error", "couldn't parse filter parameter");
        }
        SimpleFilterScript filter = new SimpleFilterScript(predicateBooleanTuple.v1(), predicateBooleanTuple.v2());
//        Event[] events = databaseAdapter.filterEvents(filter, limit, orderBy);
//        return new JsonObject().put("result", new JsonArray(Arrays.asList(events)));
        return null;
    }

    public JsonObject filterActors(JsonObject query) {
        Integer limit = query.getInteger("limit");
        String orderBy = query.getString("orderBy");
        JsonObject filterJson = query.getJsonObject("filter");
        if (filterJson != null) {
            return new JsonObject().put("error", "filter parameter is required");
        }
        Tuple<Predicate, Boolean> predicateBooleanTuple;
        try {
            predicateBooleanTuple = generatePredicate(filterJson);
        } catch (Exception e) {
            return new JsonObject().put("error", "couldn't parse filter parameter");
        }
        SimpleFilterScript filter = new SimpleFilterScript(predicateBooleanTuple.v1(), predicateBooleanTuple.v2());
//        Actor[] actors = databaseAdapter.filterActors(filter, limit, orderBy);
//        return new JsonObject().put("result", new JsonArray(actors));
        return null;
    }

    public void register(RouteMatcher.MicroRouteMatcher routeMatcher) {
//        mapRequest("/filter/actor", json -> filterActors(json), o -> ((JsonObject) o).encode());
//        mapRequest("/filter/event", json -> filterRequestHandler.filterEvents(json), o -> ((JsonObject) o).encode());

    }
}
