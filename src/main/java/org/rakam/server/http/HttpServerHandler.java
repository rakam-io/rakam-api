package org.rakam.server.http;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/10/14 12:38.
 */

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.CharsetUtil;

import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


public class HttpServerHandler extends ChannelInboundHandlerAdapter {
    private RakamHttpRequest request;
    private RouteMatcher routes;
    private StringBuilder body = new StringBuilder(2 << 15);

    public HttpServerHandler(RouteMatcher routes) {
        this.routes = routes;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        body.delete(0, body.length());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof io.netty.handler.codec.http.HttpRequest) {
            this.request = new RakamHttpRequest(ctx, (io.netty.handler.codec.http.HttpRequest) msg);

            if (HttpHeaders.is100ContinueExpected(request)) {
                ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
            } else {
                routes.handle(request);
            }
        } else if (msg instanceof DefaultLastHttpContent) {
            HttpContent chunk = (HttpContent) msg;
            if (chunk.content().isReadable()) {
                String s = chunk.content().toString(CharsetUtil.UTF_8);
                if (body == null) {
                    request.handleBody(s);
                } else {
                    body.append(s);
                    request.handleBody(body.toString());
                }
                chunk.release();
            }
        } else if (msg instanceof HttpContent) {
            HttpContent chunk = (HttpContent) msg;
            if (chunk.content().isReadable()) {
                String s = chunk.content().toString(CharsetUtil.UTF_8);
                if (body == null) {
                    body = new StringBuilder(s);
                } else {
                    body.append(s);
                }
                chunk.release();
            }

        } else if (msg instanceof WebSocketFrame) {
            routes.handle(ctx, (WebSocketFrame) msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}


