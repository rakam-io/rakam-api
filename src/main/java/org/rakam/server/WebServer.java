package org.rakam.server;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import io.netty.handler.codec.http.QueryStringDecoder;
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
                    final JsonObject main = new JsonObject();

                    if (requestPath.equals("/rule/add")) {
                        main.putString("_action", "add");

                        if (!httpServerRequest.method().equals("POST")) {
                            returnError(httpServerRequest, "POST request is required.", 400);
                            return;
                        }
                        httpServerRequest.bodyHandler(new Handler<Buffer>() {
                            @Override
                            public void handle(Buffer buff) {
                                String contentType = httpServerRequest.headers().get("Content-Type");
                                JsonObject json;
                                if ("application/x-www-form-urlencoded".equals(contentType)) {
                                    QueryStringDecoder qsd = new QueryStringDecoder(buff.toString(), false);
                                    json = new JsonObject();
                                } else if ("application/json".equals(contentType)) {
                                    json = new JsonObject(buff.toString());
                                } else {
                                    returnError(httpServerRequest, "content type must be one of [application/x-www-form-urlencoded, application/json]", 400);
                                    return;
                                }

                                main.putObject("rule", json);
                                vertx.eventBus().send("analysisRuleCrud", main, new Handler<Message<JsonObject>>() {
                                    @Override
                                    public void handle(Message<JsonObject> event) {
                                        httpServerRequest.response().end(event.body().encode());
                                    }
                                });
                            }
                        });
                    } else if (requestPath.equals("/rule/list")) {
                        if (!httpServerRequest.method().equals("POST")) {
                            returnError(httpServerRequest, "POST request is required.", 400);
                            return;
                        }
                        main.putString("_action", "list");
                        httpServerRequest.bodyHandler(new Handler<Buffer>() {
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
                                main.putObject("rule", json);

                                String project = json.getString("project");
                                if (project == null) {
                                    returnError(httpServerRequest, "project parameter must be specified.", 400);
                                    return;
                                }
                                main.putString("project", project);
                                vertx.eventBus().send("analysisRuleCrud", main, new Handler<Message<JsonObject>>() {
                                    @Override
                                    public void handle(Message<JsonObject> event) {
                                        httpServerRequest.response().end(event.body().encode());
                                    }
                                });
                            }
                        });
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


}
