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

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    @Path("/frame")
    @GET
    public void frame(RakamHttpRequest request) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        String data = "<!DOCTYPE html> \n" +
                "<html>\n" +
                "   <body>\n" +
                "  \t  <script>\n" +
                "        var frame;\n" +
                "        window.onerror = function(message, url, lineNumber) {\n" +
                "            console.log(121);window.parent.postMessage({\n" +
                "                type: 'error',\n" +
                "                message: message,\n" +
                "                url: url,\n" +
                "                lineNumber: lineNumber\n" +
                "            }, '*');\n" +
                "        };\n" +
                "        window.addEventListener('message', function(e) {\n" +
                "            var data = JSON.parse(e.data);\n" +
                "            if (data.html) {\n" +
                "                if(frame) document.body.removeChild(frame);\n" +
                "                frame = document.createElement('iframe');\n" +
                "                frame.setAttribute('style', 'border:none;width:100%;height:100%;margin:0;padding:0;position:absolute;');\n" +
                "                frame.setAttribute('sandbox', 'allow-forms allow-popups allow-scripts allow-same-origin');\n" +
                "                document.body.appendChild(frame);\n" +
                "                frame.contentWindow.API_URL = data.apiUrl;\n" +
                "                frame.contentWindow.PROJECT = data.project;\n" +
                "                frame.contentWindow.document.write(data.html +'<script>window.onerror = function(message, url, lineNumber) {console.log(2);}<\\/script>');\n" +
                "                frame.contentWindow.document.close();\n" +
                "            }\n" +
                "        });\n" +
                "\t  </script>\n" +
                "   </body>\n" +
                "</html>";

        response.headers().set(CONTENT_TYPE, "text/html");
        HttpHeaders.setContentLength(response, data.length());
        request.context().write(response);
        request.context().write(Unpooled.wrappedBuffer(data.getBytes()));
        ChannelFuture lastContentFuture = request.context().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

        if (!HttpHeaders.isKeepAlive(request)) {
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Path("/render")
    @POST
    public void render(RakamHttpRequest request) {
        request.bodyHandler(body -> {
            QueryStringDecoder decoder = new QueryStringDecoder(body, false);
            Map<String, List<String>> parameters = decoder.parameters();
            String data = parameters.get("data").get(0);

            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

            response.headers().set(CONTENT_TYPE, "text/html");
            response.headers().set("Content-Security-Policy", "default-src *");
            HttpHeaders.setContentLength(response, data.length());
            request.context().write(response);
            request.context().write(Unpooled.wrappedBuffer(data.getBytes()));
            ChannelFuture lastContentFuture = request.context().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

            if (!HttpHeaders.isKeepAlive(request)) {
                lastContentFuture.addListener(ChannelFutureListener.CLOSE);
            }
        });

    }
}
