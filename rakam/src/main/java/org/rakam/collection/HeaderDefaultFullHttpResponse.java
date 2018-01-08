package org.rakam.collection;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.*;

public final class HeaderDefaultFullHttpResponse extends DefaultHttpResponse implements FullHttpResponse {
    private final ByteBuf content;
    private final HttpHeaders trailingHeaders;

    public HeaderDefaultFullHttpResponse(HttpVersion version, HttpResponseStatus status, ByteBuf content, HttpHeaders headers) {
        super(version, status);
        trailingHeaders = headers;
        this.content = content;
    }

    @Override
    public HttpHeaders trailingHeaders() {
        return trailingHeaders;
    }

    @Override
    public HttpHeaders headers() {
        return trailingHeaders;
    }

    @Override
    public ByteBuf content() {
        return content;
    }

    @Override
    public int refCnt() {
        return content.refCnt();
    }

    @Override
    public FullHttpResponse retain() {
        content.retain();
        return this;
    }

    @Override
    public FullHttpResponse retain(int increment) {
        content.retain(increment);
        return this;
    }

    @Override
    public boolean release() {
        return content.release();
    }

    @Override
    public boolean release(int decrement) {
        return content.release(decrement);
    }

    @Override
    public FullHttpResponse setProtocolVersion(HttpVersion version) {
        super.setProtocolVersion(version);
        return this;
    }

    @Override
    public FullHttpResponse setStatus(HttpResponseStatus status) {
        super.setStatus(status);
        return this;
    }

    @Override
    public FullHttpResponse copy() {
        DefaultFullHttpResponse copy = new DefaultFullHttpResponse(
                getProtocolVersion(), getStatus(), content().copy(), true);
        copy.headers().set(headers());
        copy.trailingHeaders().set(trailingHeaders());
        return copy;
    }

    @Override
    public FullHttpResponse duplicate() {
        DefaultFullHttpResponse duplicate = new DefaultFullHttpResponse(getProtocolVersion(), getStatus(),
                content().duplicate(), true);
        duplicate.headers().set(headers());
        duplicate.trailingHeaders().set(trailingHeaders());
        return duplicate;
    }
}
