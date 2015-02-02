package org.rakam.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.rakam.util.RakamException;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.HttpServerHandler;
import org.rakam.server.http.HttpService;
import org.rakam.util.JsonHelper;
import org.rakam.util.json.JsonObject;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

import static org.rakam.server.RouteMatcher.MicroRouteMatcher;

/**
 * Created by buremba on 20/12/13.
 */


public class WebServer {
    public final RouteMatcher routeMatcher;

    @Inject
    public WebServer(Set<HttpService> httpServicePlugins) {
        routeMatcher = new RouteMatcher();

        httpServicePlugins.forEach(service -> {
            MicroRouteMatcher microRouteMatcher = new MicroRouteMatcher(service.getEndPoint(), routeMatcher);
            service.register(microRouteMatcher);
        });
    }


    public void run(int port) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_BACKLOG, 1024);
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast("httpCodec", new HttpServerCodec());
                            p.addLast("serverHandler", new HttpServerHandler(routeMatcher));
                        }
                    });

            Channel ch = b.bind(port).sync().channel();

            ch.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void mapJsonRequest(MicroRouteMatcher routeMatcher, String path, Function<JsonNode, Object> supplier) {
        routeMatcher.add(path, HttpMethod.GET, (request) -> {
            final ObjectNode json = JsonHelper.generate(request.params());

            boolean prettyPrint = JsonHelper.getOrDefault(json, "prettyprint", false);
            CompletableFuture.supplyAsync(() -> {
                try {
                    return supplier.apply(json);
                } catch (Exception e) {
                    return errorMessage("error processing request " + e.getMessage(), 500);
                }
            }).thenAccept(result -> request.response(JsonHelper.encode(result, prettyPrint)).end());
        });

        routeMatcher.add(path, HttpMethod.POST, (request) -> request.bodyHandler(obj -> {
            ObjectNode json;
            try {
                json = JsonHelper.read(obj);
            } catch (IOException e) {
                returnError(request, "json couldn't parsed: "+e.getMessage(), 400);
                return;
            } catch (ClassCastException e) {
                returnError(request, "json must be an object", 400);
                return;
            }

            boolean prettyPrint = JsonHelper.getOrDefault(json, "prettyprint", false);
            CompletableFuture<Object> o = new CompletableFuture<>();

            ForkJoinPool.commonPool().execute(() -> {
                try {
                    o.complete(supplier.apply(json));
                } catch (Exception e) {
                    o.completeExceptionally(e);
                }
            });

            o.whenComplete((result, exception) -> {
                if (exception == null) {
                    request.response(JsonHelper.encode(result, prettyPrint)).end();
                } else {
                    if(exception instanceof RakamException) {
                        int statusCode = ((RakamException) exception).getStatusCode();
                        String encode = JsonHelper.encode(errorMessage(exception.getMessage(), statusCode), prettyPrint);
                        request.response(encode, HttpResponseStatus.valueOf(statusCode)).end();
                    }else {
                        ObjectNode errorMessage = errorMessage("An error occurred while processing request.", 500);
                        String encode = JsonHelper.encode(errorMessage, prettyPrint);
                        request.response(encode, HttpResponseStatus.BAD_REQUEST).end();
                    }
                }
            });
        }));
    }

    public static void returnError(RakamHttpRequest request, String message, Integer statusCode) {
        JsonObject obj = new JsonObject();
        obj.put("error", message);
        obj.put("error_code", statusCode);

        request.response(obj.encode(), HttpResponseStatus.valueOf(statusCode))
                .headers().set("Content-Type", "application/json; charset=utf-8");
        request.end();
    }

    public static ObjectNode errorMessage(String message, int statusCode) {
        return JsonHelper.jsonObject()
        .put("error", message)
        .put("error_code", statusCode);
    }

}