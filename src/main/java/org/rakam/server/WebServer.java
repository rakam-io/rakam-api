package org.rakam.server;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.rakam.util.JsonHelper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.DecodeException;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.io.ByteArrayOutputStream;
import java.util.Map;

/**
 * Created by buremba on 20/12/13.
 */


public class WebServer extends Verticle {
    Kryo kryo = new Kryo();
    private final RouteMatcher routeMatcher;

    public WebServer() {
        routeMatcher = new RouteMatcher();
        routeMatcher.get("/event", getCollectHandler());
        routeMatcher.get("/actor", getSetActorHandler());

        routeMatcher.get("/analyze/timeseries/compute", timeseriesComputeRequest());

        routeMatcher.get("/analyze", getAnalyzeRequest());
        routeMatcher.post("/analyze", postAnalyzeRequest());

        routeMatcher.get("/filter/actor", request -> sendEvent(request, "actorFilterRequest", JsonHelper.generate(request.params())));
        routeMatcher.get("/filter/event", request -> sendEvent(request, "eventFilterRequest", JsonHelper.generate(request.params())));

        routeMatcher.getStartsWith("/analysis_rule/", getRuleHandler());
        routeMatcher.postStartsWith("/analysis_rule/", postRuleHandler());

        routeMatcher.noMatch(request -> request.response().setStatusCode(404).end("404"));
    }

    private Handler<HttpServerRequest> timeseriesComputeRequest() {
        return request -> {
            final MultiMap queryParams = request.params();
            JsonObject json = JsonHelper.generate(queryParams);
            String tracker_id = queryParams.get("_tracker");

            if (tracker_id == null) {
                returnError(request, "tracker id is required", 400);
                return;
            }

            sendRawEvent(request, "collectEvent", json);
        };
    }

    private Handler<HttpServerRequest> getCollectHandler() {
        return request -> {
            final MultiMap queryParams = request.params();
            JsonObject json = JsonHelper.generate(queryParams);
            String tracker_id = queryParams.get("_tracker");

            if (tracker_id == null) {
                returnError(request, "tracker id is required", 400);
                return;
            }

            sendRawEvent(request, "collectEvent", json);
        };
    }

    private Handler<HttpServerRequest> getAnalyzeRequest() {
        return request -> sendEvent(request, "analysisRequest", JsonHelper.generate(request.params()));
    }

    private Handler<HttpServerRequest> postAnalyzeRequest() {
        return request -> request.bodyHandler(buff -> {
            String contentType = request.headers().get("Content-Type");
            JsonObject json;
            if ("application/json".equals(contentType)) {
                json = new JsonObject(buff.toString());
            } else {
                returnError(request, "content type must be one of [application/json]", 400);
                return;
            }

            sendEvent(request, "analysisRequest", json);
        });
    }

    private Handler<HttpServerRequest> getSetActorHandler() {
        return request -> {
            final MultiMap queryParams = request.params();

            String user_id = queryParams.get("_user");
            String tracker = queryParams.get("_tracker");

            if (user_id == null) {
                returnError(request, "_user parameter is required", 400);
                return;
            }

            if (tracker == null) {
                returnError(request, "_tracker parameter is required", 400);
                return;
            }

            JsonObject attrs = new JsonObject();
            for (Map.Entry<String, String> item : queryParams) {
                String key = item.getKey();
                if (!key.startsWith("_"))
                    attrs.putString(item.getKey(), item.getValue());
            }

            JsonObject json = new JsonObject().putString("user", user_id).putString("tracker", tracker).putObject("attrs", attrs);

            sendRawEvent(request, "collectActor", json);
        };
    }

    private Handler<HttpServerRequest> getRuleHandler() {
        return request -> {
            final String action = request.path().substring(6);

            JsonObject json = new JsonObject()
                    .putObject("request", JsonHelper.generate(request.params()))
                    .putString("action", action);
            sendEvent(request, "analysisRuleCrud", json);
        };
    }

    private Handler<HttpServerRequest> postRuleHandler() {
        return request -> {
            final String action = request.path().substring(request.path().lastIndexOf("/")+1);

            request.bodyHandler(buff -> {
                String contentType = request.headers().get("Content-Type");
                JsonObject json;
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
                if(!json.containsField("_tracking")) {
                    json.putString("_tracking", request.params().get("_tracking"));
                }
                sendEvent(request, "analysisRuleCrud", new JsonObject().putObject("request", json).putString("action", action));
            });
        };
    }

    public void start() {
        kryo.register(JsonObject.class);
        vertx.createHttpServer().requestHandler(routeMatcher).listen(8888);
    }

    public void returnError(HttpServerRequest httpServerRequest, String message, Integer statusCode) {
        JsonObject obj = new JsonObject();
        obj.putString("error", message);
        obj.putNumber("error_code", statusCode);
        httpServerRequest.response().putHeader("Content-Type", "application/json; charset=utf-8");
        httpServerRequest.response().end(obj.encode());
    }

    public void sendEvent(final HttpServerRequest request, final String address, final JsonObject json) {
        vertx.eventBus().send(address, json, (Message<JsonObject> event) -> {
            String debug = json.getString("_debug");
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
