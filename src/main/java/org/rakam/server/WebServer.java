package org.rakam.server;

import org.rakam.database.DatabaseAdapter;
import org.rakam.database.cassandra.CassandraAdapter;
import org.rakam.model.Actor;
import org.rakam.util.JsonHelper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by buremba on 20/12/13.
 */


public class WebServer extends Verticle {
    private DatabaseAdapter databaseAdapter;

    public void start() {
        databaseAdapter = new CassandraAdapter();

        vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest httpServerRequest) {
                httpServerRequest.response().putHeader("Content-Type", "application/json; charset=utf-8");
                final MultiMap queryParams = httpServerRequest.params();
                String tracker_id = queryParams.get("_tracker");
                JsonObject json = JsonHelper.generate(queryParams);
                json.putString("accepted_reply", "aggregation.acceptedReplyAddress");

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
                        JsonObject obj = new JsonObject();
                        obj.putString("error", "rule id is required");
                        obj.putString("error_code", "400");
                        httpServerRequest.response().putHeader("Content-Type", "application/json; charset=utf-8");
                        httpServerRequest.response().end(obj.encode());
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
                        Actor actor = databaseAdapter.getActor(tracker_id, user_id);
                        if (actor == null) {
                            String createOnError = queryParams.get("_create");
                            if (createOnError != null && createOnError.equals("1"))
                                databaseAdapter.createActor(tracker_id, user_id, JsonHelper.generate(event).encode().getBytes());
                            else {
                                JsonObject obj = new JsonObject();
                                obj.putString("error", "actor does not exist. you must set _create parameter to 1 to create the user with given properties.");
                                obj.putString("error_code", "400");
                                httpServerRequest.response().putHeader("Content-Type", "application/json; charset=utf-8");
                                httpServerRequest.response().end(obj.encode());
                                return;
                            }
                        }
                        databaseAdapter.addPropertyToActor(actor, event);
                        httpServerRequest.response().end("1");
                    } else {
                        JsonObject obj = new JsonObject();
                        obj.putString("error", "_user, property and value parameters are required");
                        obj.putString("error_code", "400");
                        httpServerRequest.response().putHeader("Content-Type", "application/json; charset=utf-8");
                        httpServerRequest.response().end(obj.encode());
                    }
                } else if (requestPath.equals("/")) {

                    if (tracker_id == null) {
                        JsonObject obj = new JsonObject();
                        obj.putString("error", "tracker id is required");
                        obj.putString("error_code", "400");
                        httpServerRequest.response().putHeader("Content-Type", "application/json; charset=utf-8");
                        httpServerRequest.response().end(obj.encode());
                        return;
                    }

                    /*
                        The current implementation change cabins monthly.
                        However the interval must be determined by the system by taking account of data frequency
                     */
                    int time_cabin = Calendar.getInstance().get(Calendar.MONTH);

                    JsonObject event = new JsonObject();
                    for (Map.Entry<String, String> item : queryParams) {
                        String key = item.getKey();
                        if (!key.startsWith("_"))
                            event.putString(item.getKey(), item.getValue());
                    }

                    databaseAdapter.addEvent(tracker_id, time_cabin, queryParams.get("_user"), event.encode().getBytes());

                    vertx.eventBus().send("aggregation.orderQueue", json);
                    httpServerRequest.response().end("1");
                }
            }
        }).listen(8888);

        vertx.eventBus().registerHandler("aggregation.acceptedReplyAddress", new Handler<Message<String>>() {
            public void handle(Message<String> message) {
                String body = message.body();
                System.out.println("The handler has been registered across the cluster ok? " + body);
            }
        });
    }


}
