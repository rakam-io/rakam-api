package org.rakam;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.rakam.analysis.AnalysisRequestHandler;
import org.rakam.analysis.AnalysisRuleCrudHandler;
import org.rakam.analysis.FilterRequestHandler;
import org.rakam.cache.DistributedAnalysisRuleMap;
import org.rakam.cluster.MemberShipListener;
import org.rakam.collection.CollectionWorker;
import org.rakam.database.AnalysisRuleDatabase;
import org.rakam.server.WebServer;
import org.vertx.java.core.Future;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

/**
 * Created by buremba on 21/12/13.
 */

public class ServiceStarter extends Verticle {
    int cpuCore = Runtime.getRuntime().availableProcessors();
    public static Injector injector;
    public static JsonObject conf;

    public void start(final Future<Void> startedResult) {
        conf = container.config();
        JsonArray plugins = conf.getArray("plugins");
        injector = Guice.createInjector(new ServiceRecipe(plugins));

        DistributedAnalysisRuleMap.merge(ServiceStarter.injector.getInstance(AnalysisRuleDatabase.class).getAllRules());

//        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);

        container.deployVerticle(WebServer.class.getName(), cpuCore, deployResult -> {
            if (deployResult.succeeded()) {
                container.logger().info(String.format("Webserver started with %s processes.", cpuCore));
                startedResult.setResult(null);
            } else {
                startedResult.setFailure(deployResult.cause());
            }
        });

        container.deployWorkerVerticle(CollectionWorker.class.getName());
        container.deployWorkerVerticle(MemberShipListener.class.getName());

        vertx.eventBus().registerHandler(DistributedAnalysisRuleMap.IDENTIFIER, new DistributedAnalysisRuleMap());
        vertx.eventBus().registerHandler(AnalysisRequestHandler.EVENT_ANALYSIS_IDENTIFIER, new AnalysisRequestHandler());

        FilterRequestHandler handler = new FilterRequestHandler();
        vertx.eventBus().registerHandler(FilterRequestHandler.ACTOR_FILTER_IDENTIFIER, handler);
        vertx.eventBus().registerHandler(FilterRequestHandler.EVENT_FILTER_IDENTIFIER, handler);

        vertx.eventBus().registerHandler(AnalysisRuleCrudHandler.IDENTIFIER, new AnalysisRuleCrudHandler(vertx));


    }

}


