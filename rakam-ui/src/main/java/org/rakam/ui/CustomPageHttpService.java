/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.ui;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/08/15 01:50.
 */
@Path("/custom-page")
@Api(value = "/custom-page", description = "Custom page module", tags = "report, ui")
public class CustomPageHttpService extends HttpService {

    private final File path;

    @Inject
    public CustomPageHttpService(RakamUIModule.RakamUIConfig config) {
        URI uri;
        try {
            uri = new URI(config.getUI());
        } catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }

        File directory = new File(uri.getHost(), uri.getPath());
        directory = new File(directory, Optional.ofNullable(config.getDirectory()).orElse("/"));

        path = new File(directory.getPath() + File.separator + "static/views/page/page_template.template");
    }

    @Path("/render")
    @POST
    public void render(RakamHttpRequest request) {
        request.bodyHandler(buff -> {
            QueryStringDecoder decoder = new QueryStringDecoder(buff, false);
            Map<String, List<String>> parameters = decoder.parameters();
            String html = parameters.get("html").get(0);
            String js = parameters.get("js").get(0);
            String css = parameters.get("css").get(0);

            String template;
            try {
                template = new String(Files.readAllBytes(path.toPath()));
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }

            String data = String.format(template, js, css, html);

            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
            HttpHeaders.setContentLength(response, data.length());
            response.headers().set(CONTENT_TYPE, "text/html");

            if (HttpHeaders.isKeepAlive(request)) {
                response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
            }

            request.context().write(response);
            request.context().write(Unpooled.wrappedBuffer(data.getBytes()));

            ChannelFuture lastContentFuture = request.context().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

            if (!HttpHeaders.isKeepAlive(request)) {
                lastContentFuture.addListener(ChannelFutureListener.CLOSE);
            }

        });
    }
}
