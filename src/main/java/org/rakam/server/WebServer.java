package org.rakam.server;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.rakam.util.JsonHelper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
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


    public void start() {
        kryo.register(JsonObject.class);

        vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest httpServerRequest) {
                httpServerRequest.response().putHeader("Content-Type", "application/json; charset=utf-8");
                httpServerRequest.response().putHeader("Access-Control-Allow-Origin", "*");
                httpServerRequest.response().putHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
                final MultiMap queryParams = httpServerRequest.params();
                String tracker_id = queryParams.get("_tracker");
                JsonObject json = JsonHelper.generate(queryParams);
                //json.putString("accepted_reply", "aggregation.acceptedReplyAddress");

                String requestPath = httpServerRequest.path();
                //RouteMatcher matcher = new RouteMatcher();
                //matcher.get("")

                if (requestPath.equals("/collect")) {
                    if (tracker_id == null) {
                        returnError(httpServerRequest, "tracker id is required", 400);
                        return;
                    }

                    ByteArrayOutputStream by = new ByteArrayOutputStream();
                    Output out = new Output(by);
                    kryo.writeObject(out, json);


                    vertx.eventBus().send("collectionRequest", out.getBuffer(), new Handler<Message<JsonObject>>() {
                        public void handle(Message<JsonObject> message) {
                            httpServerRequest.response().end("1");
                        }
                    });
                } else if (requestPath.equals("/analyze")) {
                    String ruleId = queryParams.get("id");
                    if (ruleId != null) {
                        final String debug = queryParams.get("_debug");
                        vertx.eventBus().send("analysisRequest", json, new Handler<Message<JsonObject>>() {
                            @Override
                            public void handle(Message<JsonObject> event) {
                                httpServerRequest.response().end(debug != null && debug.equals("true") ? event.body().encodePrettily() : event.body().encode());
                            }
                        });
                    } else
                        returnError(httpServerRequest, "aggregation rule id is required", 400);
                } else if (requestPath.equals("/_actor/set")) {
                    String user_id = queryParams.get("_user");

                    if (user_id != null) {
                        HashMap<String, String> event = new HashMap();
                        for (Map.Entry<String, String> item : queryParams) {
                            String key = item.getKey();
                            if (!key.startsWith("_"))
                                event.put(item.getKey(), item.getValue());
                        }
                        httpServerRequest.response().end("1");
                    } else {
                        returnError(httpServerRequest, "_user, property and value parameters are required", 400);
                    }
                } else if (requestPath.startsWith("/rule")) {
                    if (requestPath.equals("/rule/add")) {
                        if (!httpServerRequest.method().equals("POST")) {
                            returnError(httpServerRequest, "POST request is required.", 400);
                            return;
                        }
                        httpServerRequest.bodyHandler(createHandler(httpServerRequest, "add", "analysisRuleCrud"));
                    } else if (requestPath.equals("/rule/list")) {
                        if (!httpServerRequest.method().equals("POST")) {
                            returnError(httpServerRequest, "POST request is required.", 400);
                            return;
                        }
                        httpServerRequest.bodyHandler(createHandler(httpServerRequest, "list", "analysisRuleCrud"));
                    } else if (requestPath.equals("/rule/get")) {
                        if (!httpServerRequest.method().equals("POST")) {
                            returnError(httpServerRequest, "POST request is required.", 400);
                            return;
                        }
                        httpServerRequest.bodyHandler(createHandler(httpServerRequest, "get", "analysisRuleCrud"));
                    } else {
                        returnError(httpServerRequest, "404.", 404);
                    }

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

    private Handler<Buffer> createHandler(final HttpServerRequest httpServerRequest, final String action, final String endPoint) {
        Handler<Buffer> handler = new Handler<Buffer>() {
            @Override
            public void handle(Buffer buff) {
                String contentType = httpServerRequest.headers().get("Content-Type");
                JsonObject json;
                if ("application/json".equals(contentType)) {
                    json = new JsonObject(buff.toString());
                } else {
                    returnError(httpServerRequest, "content type must be one of [application/json]", 400);
                    return;
                }

                vertx.eventBus().send(endPoint, new JsonObject().putObject("request", json).putString("action", action), new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> event) {
                        httpServerRequest.response().end(event.body().encode());
                    }
                });
            }
        };
        return handler;
    }


}
