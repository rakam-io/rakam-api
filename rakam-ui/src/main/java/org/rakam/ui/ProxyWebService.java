package org.rakam.ui;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.QueryParam;

import javax.annotation.PostConstruct;
import javax.net.ssl.SSLException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.UriBuilder;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;

@Path("/ui/proxy")
public class ProxyWebService extends HttpService {
    private final AttributeKey<RakamHttpRequest> CONNECTION_ATTR = AttributeKey.newInstance("CONNECTION_ATTR");

    private Bootstrap bootstrap;
    private Bootstrap sslBootstrap;

    @PostConstruct
    public void startClient() throws SSLException {
        NioEventLoopGroup group = new NioEventLoopGroup(4);

        SslContext sslCtx = SslContextBuilder.forClient()
                .clientAuth(ClientAuth.NONE)
                .trustManager(InsecureTrustManagerFactory.INSTANCE).build();

        sslBootstrap = new Bootstrap().channel(NioSocketChannel.class)
                .group(group).handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        InetSocketAddress addr = ch.remoteAddress();
                        SslHandler sslHandler;
                        // for some hosts the hostname and port required, jdk ssl throws handshake_failure
                        if (addr != null) {
                            sslHandler = sslCtx.newHandler(ch.alloc(), addr.getHostName(), addr.getPort());
                        } else {
                            sslHandler = sslCtx.newHandler(ch.alloc());
                        }

                        ch.pipeline().addLast(sslHandler)
                                .addLast(new HttpClientCodec())
                                .addLast(new HttpContentDecompressor())
                                .addLast(new HttpObjectAggregator(10048576))
                                .addLast(new ProxyChannelInboundHandler());
                    }
                });

        bootstrap = new Bootstrap().channel(NioSocketChannel.class)
                .group(group).handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new HttpClientCodec())
                                .addLast(new HttpContentDecompressor())
                                .addLast(new HttpObjectAggregator(10048576))
                                .addLast(new ProxyChannelInboundHandler());
                    }
                });
    }

    @Path("/")
    @GET
    public void proxy(RakamHttpRequest request, @QueryParam("u") String uri) throws InterruptedException {
        URI url = UriBuilder.fromUri(uri).build();

        int port;
        if (url.getPort() != -1) {
            port = url.getPort();
        } else if (url.getScheme().equals("http")) {
            port = 80;
        } else if (url.getScheme().equals("https")) {
            port = 443;
        } else {
            request.response("invalid scheme").end();
            return;
        }

        Channel ch = (port == 443 ? sslBootstrap : bootstrap).connect(url.getHost(), port)
                .sync().channel();
        ch.attr(CONNECTION_ATTR).set(request);

        HttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url.getRawPath());
        req.headers().set(HttpHeaders.Names.HOST, url.getHost());
        req.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
        req.headers().set(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);
        req.headers().set(HttpHeaders.Names.CACHE_CONTROL, HttpHeaders.Values.NO_CACHE);
        req.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
        req.headers().set(HttpHeaders.Names.PRAGMA, HttpHeaders.Values.NO_CACHE);
        req.headers().set(HttpHeaders.Names.USER_AGENT, "rakam-ab-test-tool 0.1");
        ch.writeAndFlush(req);
    }

    private class ProxyChannelInboundHandler extends SimpleChannelInboundHandler<HttpObject> {

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
            RakamHttpRequest rakamHttpRequest = ctx.channel().attr(CONNECTION_ATTR).get();
            Channel channel = rakamHttpRequest.context().channel();
            FullHttpResponse resp = (FullHttpResponse) msg;

            String contentType = resp.headers().get(HttpHeaders.Names.CONTENT_TYPE);
            Charset charset = null;

            if (contentType != null) {
                Iterable<String> split = Splitter.on(";").trimResults().split(contentType);
                for (String item : split) {
                    List<String> charsetStr = new QueryStringDecoder("?" + item).parameters().get("charset");
                    if (charsetStr != null) {
                        charset = Charset.forName(charsetStr.get(0));
                        break;
                    }
                }
            }

            if (charset == null) {
                charset = CharsetUtil.UTF_8;
            }

            String content = resp.content().toString(charset);
            Document parse = Jsoup.parse(content, "", Parser.htmlParser());

            String url = rakamHttpRequest.params().get("u").get(0);
            parse.head().prepend(String.format("<base href='%s'>", url));
            parse.head().prepend("<link href=\"/static/components/codemirror/lib/codemirror.css\" media=\"screen\" rel=\"stylesheet\" />");
            parse.head().prepend("<link href=\"/static/embed/jquery-ui-theme.css\" media=\"screen\" rel=\"stylesheet\" />");
            parse.head().prepend("<link href=\"/static/components/bootstrap-colorpicker/css/colorpicker.css\" media=\"screen\" rel=\"stylesheet\" />");
            parse.head().prepend("<link href=\"/static/embed/rakam-inline-editor.css\" media=\"screen\" rel=\"stylesheet\" />");

            parse.head().prepend("<script src=\"/static/embed/rakam-inline-editor.js\"></script>");
            parse.head().prepend("<script src=\"/static/components/bootstrap-colorpicker/js/bootstrap-colorpicker.js\"></script>");
            parse.head().prepend("<script src=\"/static/components/codemirror/mode/xml/xml.js\"></script>");
            parse.head().prepend("<script src=\"/static/components/codemirror/mode/javascript/javascript.js\"></script>");
            parse.head().prepend("<script src=\"/static/components/codemirror/mode/css/css.js\"></script>");
            parse.head().prepend("<script src=\"/static/components/codemirror/mode/vbscript/vbscript.js\"></script>");
            parse.head().prepend("<script src=\"/static/components/codemirror/mode/htmlmixed/htmlmixed.js\"></script>");
            parse.head().prepend("<script src=\"/static/components/codemirror/lib/codemirror.js\"></script>");
            parse.head().prepend("<script src=\"/static/components/jquery-ui/jquery-ui.min.js\"></script>");
            parse.head().prepend("<script src=\"/static/components/jquery/dist/jquery.min.js\"></script>");

            byte[] bytes = parse.outerHtml().getBytes(charset);
            DefaultFullHttpResponse copy = new DefaultFullHttpResponse(
                    resp.getProtocolVersion(), resp.getStatus(), Unpooled.wrappedBuffer(bytes));
            copy.headers().set(resp.headers());
            copy.trailingHeaders().set(resp.trailingHeaders());
            copy.headers().set(HttpHeaders.Names.CONTENT_LENGTH, bytes.length);

            String location = resp.headers().get(HttpHeaders.Names.LOCATION);

            copy.headers().set("X-Frame-Options", "ALLOWALL");

            if (location != null && (location.startsWith("/") || (resp.getStatus().code() == 301 || resp.getStatus().code() == 302))) {
                if (location.startsWith("/")) {
                    location = url.trim() + location;
                }
                copy.headers().set(HttpHeaders.Names.LOCATION, CharMatcher.is('/').trimTrailingFrom("/ui/proxy?u=" + location) + '/');
            }

            channel.writeAndFlush(copy).addListener(ChannelFutureListener.CLOSE);
        }
    }
}