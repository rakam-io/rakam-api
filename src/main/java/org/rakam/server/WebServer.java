package org.rakam.server;

import org.rakam.util.JsonHelper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by buremba on 20/12/13.
 */


public class WebServer extends Verticle {

    public void start() {

        vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest httpServerRequest) {
                httpServerRequest.response().putHeader("Content-Type", "application/json; charset=utf-8");
                final MultiMap queryParams = httpServerRequest.params();
                String tracker_id = queryParams.get("_tracker");
                JsonObject json = JsonHelper.generate(queryParams);
                //json.putString("accepted_reply", "aggregation.acceptedReplyAddress");

                String requestPath = httpServerRequest.path();
                if (requestPath.equals("/_analyze")) {
                    String uuid_str = queryParams.get("_rule");
                    if (uuid_str != null) {
                        final String debug = queryParams.get("_debug");
                        vertx.eventBus().send("analysis", json, new Handler<Message<JsonObject>>() {
                            @Override
                            public void handle(Message<JsonObject> event) {
                                httpServerRequest.response().end(debug != null && debug.equals("true") ? event.body().encodePrettily() : event.body().encode());
                            }
                        });
                    } else {
                        returnError(httpServerRequest, "rule id is required", 400);
                    }
                    return;
                } else if (requestPath.equals("/_setProperty")) {
                    String user_id = queryParams.get("_user");

                    if (user_id != null) {
                        HashMap<String, String> event = new HashMap();
                        for (Map.Entry<String, String> item : queryParams) {
                            String key = item.getKey();
                            if (!key.startsWith("_"))
                                event.put(item.getKey(), item.getValue());
                        }
                        //databaseAdapter.addPropertyToActor(tracker_id, user_id, event);
                        httpServerRequest.response().end("1");
                    } else {
                        returnError(httpServerRequest, "_user, property and value parameters are required", 400);
                    }
                } else if (requestPath.equals("/ttl")) {
                    String type = queryParams.get("_type");
                    String value = queryParams.get("type");
                    String ttl_str = queryParams.get("ttl");
                    Integer ttl;
                    if (type == null || value == null) {
                        returnError(httpServerRequest, "type and value parameters are required", 400);
                        return;
                    }
                    JsonObject obj = new JsonObject();
                    obj.putString("tracker", tracker_id);
                    obj.putString("type", type);
                    obj.putString("value", value);
                    if (ttl_str == null)
                        ttl = 15;
                    else
                        try {
                            ttl = Integer.parseInt(ttl_str);
                        } catch (NumberFormatException e) {
                            returnError(httpServerRequest, "ttl value must be an integer.", 400);
                            return;
                        }
                    obj.putNumber("ttl", ttl);
                    vertx.eventBus().send("aggregation.orderQueue", json);
                } else if (requestPath.equals("/")) {

                    if (tracker_id == null) {
                        returnError(httpServerRequest, "tracker id is required", 400);
                        return;
                    }

                    JsonObject event = new JsonObject();
                    for (Map.Entry<String, String> item : queryParams) {
                        String key = item.getKey();
                        if (!key.startsWith("_"))
                            event.putString(item.getKey(), item.getValue());
                    }

                    vertx.eventBus().send("aggregation.orderQueue", json, new Handler<Message<JsonObject>>() {
                        public void handle(Message<JsonObject> message) {
                            httpServerRequest.response().end(message.body().encode());
                        }
                    });

                } else {
                    httpServerRequest.response().end("404");
                }
            }
        }).listen(8888);
        /*
        vertx.eventBus().registerHandler("aggregation.acceptedReplyAddress", new Handler<Message<String>>() {
            public void handle(Message<String> message) {
                String body = message.body();
                System.out.println("The handler has been registered across the cluster ok? " + body);
            }
        }); */
    }

    public void returnError(HttpServerRequest httpServerRequest, String message, Integer statusCode) {
        JsonObject obj = new JsonObject();
        obj.putString("error", message);
        obj.putNumber("error_code", statusCode);
        httpServerRequest.response().putHeader("Content-Type", "application/json; charset=utf-8");
        httpServerRequest.response().end(obj.encode());
    }


}
