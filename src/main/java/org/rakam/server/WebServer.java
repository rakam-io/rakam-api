package org.rakam.server;

import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.RoutingLogic;
import akka.routing.SeveralRoutees;
import com.google.inject.Injector;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.AnalysisRuleCrudService;
import org.rakam.analysis.AnalysisRuleMap;
import org.rakam.analysis.EventAnalyzer;
import org.rakam.analysis.FilterRequestHandler;
import org.rakam.analysis.HttpService;
import org.rakam.collection.actor.ActorCollector;
import org.rakam.collection.event.EventCollector;
import org.rakam.server.http.CustomHttpRequest;
import org.rakam.util.JsonHelper;
import org.rakam.util.json.DecodeException;
import org.rakam.util.json.JsonObject;
import scala.collection.immutable.IndexedSeq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.rakam.server.RouteMatcher.MicroRouteMatcher;

/**
 * Created by buremba on 20/12/13.
 */


public class WebServer {
    public final RouteMatcher routeMatcher;
    private final EventCollector eventCollector;
    private final EventAnalyzer eventAnalyzer;
    private final Executor executor;
    private final ActorCollector actorCollector;
    private final FilterRequestHandler filterRequestHandler;

    public WebServer(Injector injector, AnalysisRuleMap analysisRuleMap, ExecutorService executor) {
        eventCollector = new EventCollector(injector, analysisRuleMap);
        eventAnalyzer = new EventAnalyzer(injector, analysisRuleMap);
        filterRequestHandler = new FilterRequestHandler(injector);
        actorCollector = new ActorCollector(injector);
        routeMatcher = new RouteMatcher();
        this.executor = executor;

        routeMatcher.add(HttpMethod.POST, "/event", request -> {
            request.bodyHandler(buff -> {

                CompletableFuture.supplyAsync(() -> {
                    JsonObject json = null;
                    try {
                        json = new JsonObject(buff);
                    } catch (DecodeException e) {
                        request.response("0");
                    }
                    return eventCollector.submitEvent(json);
                }, executor)
                        .thenAccept(result -> request.response(result ? "1" : "0").end());
            });
        });
        routeMatcher.add(HttpMethod.GET, "/event", request -> {
            final JsonObject json = JsonHelper.generate(request.params());
            CompletableFuture.supplyAsync(() -> eventCollector.submitEvent(json), executor)
                    .thenAccept(result -> request.response(result ? "1" : "0").end());
        });

        mapRequest("/analyze", json -> eventAnalyzer.analyze(json), o -> ((JsonObject) o).encode());
        mapRequest("/actor", json -> actorCollector.handle(json), o -> o.toString());

        mapRequest("/filter/event", json -> filterRequestHandler.filterEvents(json), o -> ((JsonObject) o).encode());

        registerRoutes(new AnalysisRuleCrudService(injector, analysisRuleMap));
    }

    private void registerRoutes(HttpService service)  {
        final MicroRouteMatcher microRouteMatcher = new MicroRouteMatcher(service.getEndPoint(), routeMatcher);
        service.register(microRouteMatcher);
    }

    private void mapRequest(String path, Function<JsonObject, Object> supplier, Function<Object, String> resultFunc) {
        routeMatcher.add(HttpMethod.GET, path, (request) -> {
            final JsonObject json = JsonHelper.generate(request.params());
            CompletableFuture.supplyAsync(() -> supplier.apply(json), executor)
                    .thenAccept(result -> request.response(resultFunc.apply(result)).end());
        });

        routeMatcher.add(HttpMethod.POST, path, (request) -> {
            final JsonObject json = JsonHelper.generate(request.params());
            CompletableFuture.supplyAsync(() -> supplier.apply(json), executor)
                    .thenAccept(result -> request.response(resultFunc.apply(result)).end());
        });
    }

    private void returnError(CustomHttpRequest request, String message, Integer statusCode) {
        JsonObject obj = new JsonObject();
        obj.putString("error", message);
        obj.putNumber("error_code", statusCode);

        request.response(obj.encode(), HttpResponseStatus.valueOf(statusCode))
                .headers().set("Content-Type", "application/json; charset=utf-8");
    }

}