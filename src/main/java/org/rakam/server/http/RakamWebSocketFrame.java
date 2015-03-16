package org.rakam.server.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/03/15 17:03.
 */
public class RakamWebSocketFrame {
    private ChannelHandlerContext ctx;
    private WebSocketFrame frame;
    private WebSocketServerHandshaker handshaker;

    public RakamWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
        this.ctx = ctx;
        this.frame = frame;
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    public WebSocketFrame getFrame() {
        return frame;
    }
}
