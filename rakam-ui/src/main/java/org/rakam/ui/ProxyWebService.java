package org.rakam.ui;

import com.google.common.base.CharMatcher;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;
import org.rakam.plugin.IgnorePermissionCheck;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;

import javax.annotation.PostConstruct;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;

@Path("/ui/proxy")
public class ProxyWebService extends HttpService {
    private final AttributeKey<RakamHttpRequest> CONNECTION_ATTR = AttributeKey.newInstance("CONNECTION_ATTR");

    private Bootstrap bootstrap;

    @PostConstruct
    public void startClient() {
        bootstrap = new Bootstrap().channel(NioSocketChannel.class).group(new NioEventLoopGroup(4));

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                p.addLast(new HttpClientCodec());
                p.addLast(new HttpContentDecompressor());
                p.addLast(new HttpObjectAggregator(1048576));

                p.addLast(new SimpleChannelInboundHandler<HttpObject>() {

                    @Override
                    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
                        RakamHttpRequest rakamHttpRequest = ctx.channel().attr(CONNECTION_ATTR).get();
                        Channel channel = rakamHttpRequest.context().channel();
                        FullHttpResponse resp = (FullHttpResponse) msg;

                        String content = resp.content().toString(CharsetUtil.UTF_8);
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

                        byte[] bytes = parse.outerHtml().getBytes(CharsetUtil.UTF_8);
                        DefaultFullHttpResponse copy = new DefaultFullHttpResponse(
                                resp.getProtocolVersion(), resp.getStatus(), Unpooled.wrappedBuffer(bytes));
                        copy.headers().set(resp.headers());
                        copy.trailingHeaders().set(resp.trailingHeaders());
                        copy.headers().set(HttpHeaders.Names.CONTENT_LENGTH, bytes.length);

                        String location = resp.headers().get(HttpHeaders.Names.LOCATION);

                        if(location != null && (location.startsWith("/") || resp.getStatus().code() == 301)) {
                            if(location.startsWith("/")) {
                                location = url.trim()+location;
                            }
                            copy.headers().set(HttpHeaders.Names.LOCATION, CharMatcher.is('/').trimTrailingFrom("/ui/proxy?u="+location)+'/');
                        }

                        channel.writeAndFlush(copy).addListener(ChannelFutureListener.CLOSE);
                    }
                });
            }
        });
    }

    @Path("/")
    @GET
    @IgnorePermissionCheck
    public void proxy(RakamHttpRequest request) {
        URI url = UriBuilder.fromUri(request.params().get("u").get(0)).build();

        Channel ch = null;
        try {
            ch = bootstrap.connect(url.getHost(), 80).sync().channel();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ch.attr(CONNECTION_ATTR).set(request);

        HttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url.getRawPath());
        req.headers().set(HttpHeaders.Names.HOST, url.getHost());
        req.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
        req.headers().set(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);
        ch.writeAndFlush(req);
    }
}
