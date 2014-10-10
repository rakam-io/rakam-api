package org.rakam.analysis;

import org.rakam.ServiceStarter;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.DistributedCacheAdapter;
import org.rakam.constant.AggregationAnalysis;
import org.rakam.database.DatabaseAdapter;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import static org.rakam.util.JsonHelper.returnError;

/**
 * Created by buremba <Burak Emre Kabakcı> on 03/09/14 02:02.
 */
public class AnalysisRequestHandler implements Handler<Message<JsonObject>> {
    public final static String EVENT_ANALYSIS_IDENTIFIER = "analysisRequest";

    private static final DatabaseAdapter databaseAdapter = ServiceStarter.injector.getInstance(DatabaseAdapter.class);
    private static final CacheAdapter cacheAdapter = ServiceStarter.injector.getInstance(DistributedCacheAdapter.class);

    @Override
    public void handle(Message<JsonObject> event) {
        JsonObject query = event.body();

        final AggregationAnalysis aggAnalysis;
        try {
            aggAnalysis = AggregationAnalysis.get(query.getString("analysis_type"));
        } catch (IllegalArgumentException|NullPointerException e) {
            event.reply(returnError("analysis_type parameter is empty or doesn't exist."));
            return;
        }

        String tracker = query.getString("tracker");

        if (tracker == null) {
            event.reply(returnError("tracker parameter is required."));
            return;
        }

        event.reply(new EventAnalyzer(cacheAdapter, databaseAdapter).analyze(aggAnalysis, tracker, query));

    }
}