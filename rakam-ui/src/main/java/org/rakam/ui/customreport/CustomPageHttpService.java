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
package org.rakam.ui.customreport;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import org.rakam.config.EncryptionConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.ui.page.CustomPageDatabase;
import org.rakam.ui.user.WebUserHttpService;
import org.rakam.util.IgnorePermissionCheck;
import org.rakam.util.JsonHelper;
import org.rakam.util.JsonResponse;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.rakam.util.JsonHelper.encode;


@Path("/custom-page")
@IgnoreApi
@Api(value = "/custom-page", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
public class CustomPageHttpService extends HttpService {
    private final CustomPageDatabase database;
    private final EncryptionConfig encryptionConfig;

    @Inject
    public CustomPageHttpService(CustomPageDatabase database, EncryptionConfig encryptionConfig) {
        this.database = database;
        this.encryptionConfig = encryptionConfig;
    }

    @Path("/frame")
    @GET
    @IgnorePermissionCheck
    public void frame(RakamHttpRequest request) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        String data = "<!DOCTYPE html> \n" +
                "<html>\n" +
                "   <body>\n" +
                "  \t  <script>\n" +
                "        var frame;\n" +
                "        window.onerror = function(message, url, lineNumber) {\n" +
                "            window.parent.postMessage({\n" +
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
                "                frame.contentWindow.document.write(data.html);\n" +
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

    @Path("/save")
    @POST
    @ApiOperation(value = "Save Report", authorizations = @Authorization(value = "read_key"),
            response = JsonResponse.class, request = CustomPageDatabase.Page.class)
    public void save(RakamHttpRequest request) {
        request.bodyHandler(body -> {
            CustomPageDatabase.Page report = JsonHelper.read(body, CustomPageDatabase.Page.class);

            Optional<Integer> user = request.cookies().stream().filter(a -> a.name().equals("session")).findFirst()
                    .map(cookie -> WebUserHttpService.extractUserFromCookie(cookie.value(), encryptionConfig.getSecretKey()));

            if (!user.isPresent()) {
                request.response(encode(JsonResponse.error("Unauthorized")), UNAUTHORIZED).end();
            } else {
                database.save(user.get(), report);
                request.response(encode(JsonResponse.success()), OK).end();
            }
        });
    }

    @Path("/delete")
    @ApiOperation(value = "Delete Report", authorizations = @Authorization(value = "read_key"))
    @JsonRequest
    public JsonResponse delete(@ApiParam(name = "project") String project,
                               @ApiParam(name = "name") String name) {
        database.delete(project, name);
        return JsonResponse.success();
    }

    @Path("/get")
    @ApiOperation(value = "Get Report", authorizations = @Authorization(value = "read_key"))
    @JsonRequest
    public Map<String, String> get(@ApiParam(name = "project") String project,
                                   @ApiParam(name = "slug") String slug) {
        return database.get(project, slug);
    }

    @Path("/display/*")
    @IgnorePermissionCheck
    @GET
    public void display(RakamHttpRequest request) {
        String path = request.path().substring(21);
        String[] projectCustomPage = path.split("/", 3);
        byte[] bytes;
        try {
            InputStream file = database.getFile(projectCustomPage[0], projectCustomPage[1], projectCustomPage[2]);
            if(file == null) {
                request.response(NOT_FOUND.reasonPhrase(), NOT_FOUND).end();
            }
            bytes = ByteStreams.toByteArray(file);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        response.headers().set(CONTENT_TYPE, "text/html");
        HttpHeaders.setContentLength(response, bytes.length);
        request.context().write(response);
        request.context().write(Unpooled.wrappedBuffer(bytes));
        ChannelFuture lastContentFuture = request.context().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

        if (!HttpHeaders.isKeepAlive(request)) {
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Path("/list")
    @ApiOperation(value = "Get Report", authorizations = @Authorization(value = "read_key"))
    @JsonRequest
    public List<CustomPageDatabase.Page> list(@ApiParam(name = "project") String project) {
        return database.list(project);
    }
}
