package org.rakam.server.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.rakam.util.JsonHelper;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_HEADERS;
import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.util.CharsetUtil.UTF_8;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/10/14 19:04.
 */
public class RakamHttpRequest implements HttpRequest {
    private final ChannelHandlerContext ctx;
    private io.netty.handler.codec.http.HttpRequest request;
    protected FullHttpResponse response;
    private Consumer<String> bodyHandler;
    private ByteBuf emptyBuffer  = Unpooled.wrappedBuffer(new byte[0]);

    private String path;
    private Map<String, List<String>> params;

    public RakamHttpRequest(ChannelHandlerContext ctx, HttpRequest request) {
        this.ctx = ctx;
        this.request = request;
    }

    HttpRequest getRequest() {
        return request;
    }

    ChannelHandlerContext getContext() {
        return ctx;
    }

    @Override
    public HttpMethod getMethod() {
        return request.getMethod();
    }

    @Override
    public HttpRequest setMethod(HttpMethod method) {
        return request.setMethod(method);
    }

    @Override
    public String getUri() {
        return request.getUri();
    }

    @Override
    public HttpRequest setUri(String uri) {
        return request.setUri(uri);
    }

    @Override
    public HttpVersion getProtocolVersion() {
        return request.getProtocolVersion();
    }


    @Override
    public io.netty.handler.codec.http.HttpRequest setProtocolVersion(HttpVersion version) {
        return request.setProtocolVersion(version);
    }

    @Override
    public HttpHeaders headers() {
        return request.headers();
    }

    public RakamHttpRequest putHeader(String key, Object value) {
        response.headers().set(key, value);
        return this;
    }

    @Override
    public DecoderResult getDecoderResult() {
        return request.getDecoderResult();
    }

    @Override
    public void setDecoderResult(DecoderResult result) {
        request.setDecoderResult(result);
    }

    public void bodyHandler(Consumer<String> function) {
        bodyHandler = function;
    }

    public RakamHttpRequest response(String content) {
        final ByteBuf byteBuf = Unpooled.wrappedBuffer(content.getBytes(UTF_8));
        response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, byteBuf);
        return this;
    }

    public RakamHttpRequest response(byte[] content) {
        final ByteBuf byteBuf = Unpooled.copiedBuffer(content);
        response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, byteBuf);
        return this;
    }

    public RakamHttpRequest response(String content, HttpResponseStatus status) {
        final ByteBuf byteBuf = Unpooled.wrappedBuffer(content.getBytes(UTF_8));
        response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, byteBuf);
        return this;
    }

    public RakamHttpRequest jsonResponse(Object content, HttpResponseStatus status) {
        List<String> debug = params().get("_debug");
        String data = JsonHelper.encode(content, debug != null ? debug.get(0).equals(true): false);
        final ByteBuf byteBuf = Unpooled.wrappedBuffer(data.getBytes(UTF_8));
        response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, byteBuf);
        return this;
    }

    public RakamHttpRequest jsonResponse(Object content) {
        jsonResponse(content, HttpResponseStatus.OK);
        return this;
    }

    public Map<String, List<String>> params() {
        if (params == null) {
            QueryStringDecoder qs = new QueryStringDecoder(request.getUri());
            path = qs.path();
            params = qs.parameters();
        }
        return params;
    }

    public String path() {
        if (path == null) {
            QueryStringDecoder qs = new QueryStringDecoder(request.getUri());
            path = qs.path();
            params = qs.parameters();
        }
        return path;
    }

    public void end() {
        if(response == null) {
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, emptyBuffer);
        }
        boolean keepAlive = HttpHeaders.isKeepAlive(request);

        response.headers().set(CONTENT_TYPE, "application/json; charset=utf-8");
        response.headers().set(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        response.headers().set(ACCESS_CONTROL_ALLOW_HEADERS, "Origin, X-Requested-With, Content-Type, Accept");

        if (keepAlive) {
            response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
            response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);

            ctx.writeAndFlush(response);
        } else {
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    public void handleBody(String s) {
        if (bodyHandler != null) {
            bodyHandler.accept(s);
        }
    }

    public StreamResponse streamResponse() {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(CONTENT_TYPE, "text/event-stream");
        response.headers().set(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        ctx.writeAndFlush(response);
        return new StreamResponse(ctx);
    }

    public static class StreamResponse {
        private final ChannelHandlerContext ctx;

        public StreamResponse(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        public StreamResponse send(String event, String data) {
            if(ctx.isRemoved()){
                throw new IllegalStateException();
            }
            ByteBuf msg = Unpooled.wrappedBuffer(("event:"+event + "\ndata:" + data + "\n\n").getBytes(UTF_8));
            ctx.writeAndFlush(msg);
            return this;
        }

        public StreamResponse send(String event, Object data) {
            String json;
            try {
                json = JsonHelper.encodeSafe(data);
            } catch (JsonProcessingException e) {
                send(event, "couldn't serialize json object: " + e.getMessage());
                return this;
            }
            return send(event, json);
        }

        public void end() {
            ctx.close().awaitUninterruptibly();
        }
    }
}
