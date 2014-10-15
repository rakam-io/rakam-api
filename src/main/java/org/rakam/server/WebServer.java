package org.rakam.server;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.rakam.analysis.AnalysisRequestHandler;
import org.rakam.analysis.AnalysisRuleCrudHandler;
import org.rakam.analysis.FilterRequestHandler;
import org.rakam.collection.CollectionWorker;
import org.rakam.util.JsonHelper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.DecodeException;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.io.ByteArrayOutputStream;

/**
 * Created by buremba on 20/12/13.
 */


public class WebServer extends Verticle {
    Kryo kryo = new Kryo();
    private final RouteMatcher routeMatcher;

    public void start() {
        kryo.register(JsonObject.class);
        vertx.createHttpServer().requestHandler(routeMatcher).listen(8888);
    }

    public WebServer() {
        routeMatcher = new RouteMatcher();

        routeMatcher.get("/event", request -> sendRawEvent(request, CollectionWorker.collectEvent, JsonHelper.generate(request.params())));
        routeMatcher.post("/event", request -> {
            request.bodyHandler(buff -> {
                String contentType = request.headers().get("Content-Type");
                final JsonObject json;
                if ("application/json".equals(contentType)) {
                    try {
                        json = new JsonObject(buff.toString());
                    } catch (DecodeException e) {
                        returnError(request, "json couldn't decoded.", 401);
                        return;
                    }
                } else {
                    returnError(request, "content type must be one of [application/json]", 400);
                    return;
                }
                ByteArrayOutputStream by = new ByteArrayOutputStream();
                Output out = new Output(by);
                kryo.writeObject(out, json);

                vertx.eventBus().send(CollectionWorker.collectEvent, by.toByteArray(), (Message<JsonObject> event) -> {
                    final Number status = json.getNumber("status");
                    if(status!=null) {
                        request.response().setStatusCode(status.shortValue());
                    }

                    vertx.eventBus().send(CollectionWorker.collectEvent, out.getBuffer(), (Message<byte[]> e) -> {
                            String chunk = new String(e.body());
                            request.response().end(chunk);
                    });
                });

            });
        });

        mapRequest("/actor", CollectionWorker.collectActor);
        mapRequest("/analyze/timeseries/compute", AnalysisRequestHandler.EVENT_ANALYSIS_IDENTIFIER);
        mapRequest("/analyze", AnalysisRequestHandler.EVENT_ANALYSIS_IDENTIFIER);
        mapRequest("/filter/actor", FilterRequestHandler.EVENT_FILTER_IDENTIFIER);
        mapRequest("/filter/event", FilterRequestHandler.ACTOR_FILTER_IDENTIFIER);

        mapRequestStartsWith("/analysis_rule/", AnalysisRuleCrudHandler.IDENTIFIER);

        routeMatcher.noMatch(request -> request.response().setStatusCode(404).end("404"));
    }

    private void mapRequest(String path, String event) {
        routeMatcher.get(path, request -> sendGetEvent(request, event));
        routeMatcher.post(path, request -> sendPostEvent(request, event, null));
    }

    private void mapRequestStartsWith(String path, String event) {
        routeMatcher.getStartsWith(path, request -> {
            final String action = request.path().substring(path.length());
            JsonObject json = new JsonObject().putObject("request", JsonHelper.generate(request.params())).putString("action", action);
            sendEvent(request, event, json);
        });
        routeMatcher.postStartsWith(path, request -> {
            final String action = request.path().substring(path.length());
            sendPostEvent(request, event, new JsonObject().putString("action", action));
        });
    }

    private void sendPostEvent(HttpServerRequest request, final String address, JsonObject container) {
        request.bodyHandler(buff -> {
            String contentType = request.headers().get("Content-Type");
            final JsonObject json;
            if ("application/json".equals(contentType)) {
                try {
                    json = new JsonObject(buff.toString());
                } catch (DecodeException e) {
                    returnError(request, "json couldn't decoded.", 401);
                    return;
                }
            } else {
                returnError(request, "content type must be one of [application/json]", 400);
                return;
            }
            vertx.eventBus().send(address, container!=null ? container.putObject("request", json) : json, (Message<JsonObject> event) -> {
                Boolean debug = json.getBoolean("_debug");
                final Number status = json.getNumber("status");
                if(status!=null) {
                    request.response().setStatusCode(status.shortValue());
                }
                request.response().end(debug != null && debug ? event.body().encodePrettily() : event.body().encode());
            });

        });
    }

    private void returnError(HttpServerRequest httpServerRequest, String message, Integer statusCode) {
        JsonObject obj = new JsonObject();
        obj.putString("error", message);
        obj.putNumber("error_code", statusCode);
        httpServerRequest.response().setStatusCode(statusCode);
        httpServerRequest.response().putHeader("Content-Type", "application/json; charset=utf-8");
        httpServerRequest.response().end(obj.encode());
    }

    private void sendGetEvent(HttpServerRequest request, final String address) {
        sendEvent(request, address, JsonHelper.generate(request.params()));
    }

    private void sendEvent(final HttpServerRequest request, final String address, final JsonObject json) {
        vertx.eventBus().send(address, json, (Message<JsonObject> event) -> {
            final String debug = json.getString("_debug");
            final Number status = json.getNumber("status");
            if(status!=null) {
                request.response().setStatusCode(status.shortValue());
            }
            request.response().end(debug != null && debug.equals("true") ? event.body().encodePrettily() : event.body().encode());
        });
    }

    public void sendRawEvent(final HttpServerRequest request, final String address, final JsonObject json) {
        ByteArrayOutputStream by = new ByteArrayOutputStream();
        Output out = new Output(by);
        kryo.writeObject(out, json);

        Handler<Message<byte[]>> replyHandler = (Message<byte[]> event) -> {
            String chunk = new String(event.body());
            request.response().end(chunk);
        };
        vertx.eventBus().send(address, out.getBuffer(), replyHandler);
    }
}
