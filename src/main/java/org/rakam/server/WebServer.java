package org.rakam.server;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.Map;

/**
 * Created by buremba on 20/12/13.
 */


public class WebServer extends Verticle {

    public void start() {

        vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest httpServerRequest) {
                final MultiMap queryParams = httpServerRequest.params();
                String tracker_id = queryParams.get("tracker");

                if (httpServerRequest.path().equals("/_analyze")) {
                    String uuid_str = queryParams.get("_uuid");
                    if (uuid_str != null) {
                        vertx.eventBus().send("analysis", MultiMaptoJsonObject(queryParams), new Handler<Message<JsonObject>>() {
                            @Override
                            public void handle(Message<JsonObject> event) {
                                httpServerRequest.response().putHeader("Content-Type", "application/json; charset=utf-8");
                                String debug = queryParams.get("_debug");

                                httpServerRequest.response().end(debug != null && debug.equals("true") ? event.body().encodePrettily() : event.body().encode());
                            }
                        });
                    }
                    return;
                }

                if (tracker_id == null) {
                    JsonObject obj = new JsonObject();
                    obj.putString("error", "tracker id is required");
                    obj.putString("error_code", "400");
                    httpServerRequest.response().putHeader("Content-Type", "application/json; charset=utf-8");
                    httpServerRequest.response().end(obj.encode());
                    return;
                }
                vertx.eventBus().send("aggregation.orderQueue", MultiMaptoJsonObject(queryParams));
                httpServerRequest.response().end("1");
            }
        }).listen(8888);

        vertx.eventBus().registerHandler("aggregation.acceptedReplyAddress", new Handler<Message<String>>() {
            public void handle(Message<String> message) {
                String body = message.body();
                System.out.println("The handler has been registered across the cluster ok? " + body);
            }
        });
    }

    public JsonObject MultiMaptoJsonObject(MultiMap map) {
        JsonObject obj = new JsonObject();
        for (Map.Entry<String, String> item : map) {

            obj.putString(item.getKey(), item.getValue());

        }
        obj.putString("accepted_reply", "aggregation.acceptedReplyAddress");
        return obj;
    }
}
