package org.rakam.server.http;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/10/14 12:38.
 */

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.CharsetUtil;
import org.rakam.server.RouteMatcher;

import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


public class HttpServerHandler extends ChannelInboundHandlerAdapter {
    private CustomHttpRequest request;
    private RouteMatcher routes;

    public HttpServerHandler(RouteMatcher routes) {
        this.routes = routes;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
//        ctx.flush();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpRequest) {
            this.request = new CustomHttpRequest(ctx, (HttpRequest) msg);

            if (HttpHeaders.is100ContinueExpected(request)) {
                ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
            } else {
                routes.handle(request);
            }
        } else if (msg instanceof HttpContent) {
            HttpContent chunk = (HttpContent) msg;
            if (chunk.content().isReadable()) {
                request.handleBody(chunk.content().toString(CharsetUtil.UTF_8));
                chunk.release();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}


