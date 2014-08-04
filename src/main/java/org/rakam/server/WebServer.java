package org.rakam.server;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.rakam.util.JsonHelper;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.DecodeException;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by buremba on 20/12/13.
 */


public class WebServer extends Verticle {
    Kryo kryo = new Kryo();
    private final RouteMatcher routeMatcher;

    public WebServer() {
        routeMatcher = new RouteMatcher();
        routeMatcher.get("/collect", request -> {
            final MultiMap queryParams = request.params();
            JsonObject json = JsonHelper.generate(queryParams);
            String tracker_id = queryParams.get("_tracker");
            if (tracker_id == null) {
                returnError(request, "tracker id is required", 400);
                return;
            }

            ByteArrayOutputStream by = new ByteArrayOutputStream();
            Output out = new Output(by);

            kryo.writeObject(out, json);

            vertx.eventBus().send("collectionRequest", out.getBuffer(), (Message<byte[]> message) -> request.response().end("1"));
        });
        routeMatcher.get("/analyze", request -> sendEvent(request, "analysisRequest", JsonHelper.generate(request.params())));
        routeMatcher.post("/analyze", request -> request.bodyHandler(buff -> {
            String contentType = request.headers().get("Content-Type");
            JsonObject json;
            if ("application/json".equals(contentType)) {
                json = new JsonObject(buff.toString());
            } else {
                returnError(request, "content type must be one of [application/json]", 400);
                return;
            }

            sendEvent(request, "analysisRequest", json);
        }));
        routeMatcher.get("/_actor/set", request -> {
            final MultiMap queryParams = request.params();

            String user_id = queryParams.get("_user");

            if (user_id != null) {
                HashMap<String, String> event = new HashMap();
                for (Map.Entry<String, String> item : queryParams) {
                    String key = item.getKey();
                    if (!key.startsWith("_"))
                        event.put(item.getKey(), item.getValue());
                }
                request.response().end("1");
            } else {
                returnError(request, "_user, property and value parameters are required", 400);
            }
        });

        routeMatcher.getStartsWith("/rule/", request -> {
            final String action = request.path().substring(6);

            JsonObject json = new JsonObject().putObject("request", JsonHelper.generate(request.params())).putString("action", action);
            sendEvent(request, "analysisRuleCrud", json);
        });
        routeMatcher.postStartsWith("/rule/", request -> {
            final String action = request.path().substring(6);
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
                sendEvent(request, "analysisRuleCrud", new JsonObject().putObject("request", json).putString("action", action));
            });
        });
        routeMatcher.noMatch(request -> request.response().end("<h1>404</h1>"));
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


}
