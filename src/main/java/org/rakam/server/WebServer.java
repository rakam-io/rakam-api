package org.rakam.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import io.airlift.log.Logger;
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
import org.rakam.server.http.HttpRequestHandler;
import org.rakam.server.http.HttpServerHandler;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.JsonRequest;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.rakam.util.json.JsonObject;

import javax.ws.rs.Path;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static java.lang.String.format;
import static org.rakam.server.RouteMatcher.MicroRouteMatcher;
import static org.rakam.util.Lambda.produceLambda;

/**
 * Created by buremba on 20/12/13.
 */


public class WebServer {
    final static Logger LOGGER = Logger.get(WebServer.class);
    private static String REQUEST_HANDLER_ERROR_MESSAGE = "Request handler method %s.%s couldn't converted to request handler lambda expression: \n %s";

    public final RouteMatcher routeMatcher;
    EventLoopGroup bossGroup;
    EventLoopGroup workerGroup;

    @Inject
    public WebServer(Set<HttpService> httpServicePlugins) {
        routeMatcher = new RouteMatcher();

        httpServicePlugins.forEach(service -> {
            String mainPath = service.getClass().getAnnotation(Path.class).value();
            if(mainPath == null) {
                throw new IllegalStateException(format("Classes that implement HttpService must have %s annotation.", Path.class.getCanonicalName()));
            }
            MicroRouteMatcher microRouteMatcher = new MicroRouteMatcher(routeMatcher, mainPath);
            for (Method method : service.getClass().getMethods()) {
                Path annotation = method.getAnnotation(Path.class);

                if (annotation != null) {
                    String lastPath = annotation.value();
                    JsonRequest jsonRequest = method.getAnnotation(JsonRequest.class);
                    boolean mapped = false;
                    for (Annotation ann : method.getAnnotations()) {
                        javax.ws.rs.HttpMethod methodAnnotation = ann.annotationType().getAnnotation(javax.ws.rs.HttpMethod.class);

                        if (methodAnnotation != null) {
                            HttpRequestHandler handler = null;
                            HttpMethod httpMethod = HttpMethod.valueOf(methodAnnotation.value());
                            try {
                                if (jsonRequest == null) {
                                    handler = generateRequestHandler(service, method);
                                } else if (httpMethod == HttpMethod.POST) {
                                    mapped = true;
                                    handler = createPostRequestHandler(service, method);
                                } else if (httpMethod == HttpMethod.GET) {
                                    mapped = true;
                                    handler = createGetRequestHandler(service, method);
                                }
                            } catch (Throwable e) {
                                throw new RuntimeException(format(REQUEST_HANDLER_ERROR_MESSAGE,
                                        method.getClass().getName(), method.getName(), e));
                            }

                            microRouteMatcher.add(lastPath, httpMethod, handler);
                            if (lastPath.equals("/"))
                                microRouteMatcher.add("", httpMethod, handler);
                        }
                    }
                    if (!mapped && jsonRequest != null) {
                        try {
                            microRouteMatcher.add(lastPath, HttpMethod.POST, createPostRequestHandler(service, method));
                            microRouteMatcher.add(lastPath, HttpMethod.GET, createGetRequestHandler(service, method));
                        } catch (Throwable e) {
                            throw new RuntimeException(format(REQUEST_HANDLER_ERROR_MESSAGE,
                                    method.getDeclaringClass().getName(), method.getName(), e));
                        }
                    }
                }
            }
        });
    }

    private static BiFunction<HttpService, JsonNode, Object> generateJsonRequestHandler(Method method) throws Throwable {
        if (!Object.class.isAssignableFrom(method.getReturnType()) ||
                method.getParameterCount() != 1 ||
                !method.getParameterTypes()[0].equals(JsonNode.class))
            throw new IllegalStateException(format("The signature of @JsonRequest methods must be [Object (%s)]", JsonNode.class.getCanonicalName()));

        MethodHandles.Lookup caller = MethodHandles.lookup();
        return produceLambda(caller, method, BiFunction.class.getMethod("apply", Object.class, Object.class));
    }

    private static HttpRequestHandler generateRequestHandler(HttpService service, Method method) throws Throwable {
        if (!method.getReturnType().equals(void.class) ||
                method.getParameterCount() != 1 ||
                !method.getParameterTypes()[0].equals(RakamHttpRequest.class))
            throw new IllegalStateException(format("The signature of HTTP request methods must be [void ()]", RakamHttpRequest.class.getCanonicalName()));

        MethodHandles.Lookup caller = MethodHandles.lookup();

        if (Modifier.isStatic(method.getModifiers())) {
            Consumer<RakamHttpRequest> lambda;
            lambda = produceLambda(caller, method, Consumer.class.getMethod("accept", Object.class));
            return request -> lambda.accept(request);
        } else {
            BiConsumer<HttpService, RakamHttpRequest> lambda;
            lambda = produceLambda(caller, method, BiConsumer.class.getMethod("accept", Object.class, Object.class));
            return request -> lambda.accept(service, request);
        }
    }

    private static HttpRequestHandler createPostRequestHandler(HttpService service, Method method) throws Throwable {

        BiFunction<HttpService, JsonNode, Object> function = generateJsonRequestHandler(method);
        return (request) -> request.bodyHandler(obj -> {
            JsonNode json;
            try {
                json = JsonHelper.read(obj);
            } catch (IOException e) {
                returnError(request, "json couldn't parsed: " + e.getMessage(), 400);
                return;
            } catch (ClassCastException e) {
                returnError(request, "json must be an object", 400);
                return;
            }

            handleJsonRequest(service, request, function, json);
        });
    }

    private static HttpRequestHandler createGetRequestHandler(HttpService service, Method method) throws Throwable {
        BiFunction<HttpService, JsonNode, Object> function = generateJsonRequestHandler(method);
        return (request) -> {
            ObjectNode json = JsonHelper.generate(request.params());
            handleJsonRequest(service, request, function, json);
        };
    }

    private static void handleJsonRequest(HttpService serviceInstance, RakamHttpRequest request, BiFunction<HttpService, JsonNode, Object> function, JsonNode json) {
        boolean prettyPrint = JsonHelper.getOrDefault(json, "prettyprint", false);

        try {
            Object apply = function.apply(serviceInstance, json);
            String response = JsonHelper.encode(apply, prettyPrint);
            request.response(response).end();
        } catch (RakamException e) {
            int statusCode = e.getStatusCode();
            String encode = JsonHelper.encode(errorMessage(e.getMessage(), statusCode), prettyPrint);
            request.response(encode, HttpResponseStatus.valueOf(statusCode)).end();
        } catch (Exception e) {
            ObjectNode errorMessage = errorMessage("error processing request " + e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
            request.response(JsonHelper.encode(errorMessage, prettyPrint), HttpResponseStatus.BAD_REQUEST).end();
        }
    }


    public void run(int port) throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
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