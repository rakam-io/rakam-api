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

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.*;
import org.rakam.ui.ProtectEndpoint;
import org.rakam.ui.UIPermissionParameterProvider;
import org.rakam.ui.page.CustomPageDatabase;
import org.rakam.util.RakamException;
import org.rakam.util.SuccessMessage;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

@Path("/ui/custom-page")
@IgnoreApi
@Api(value = "/ui/custom-page", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
public class CustomPageHttpService
        extends HttpService {
    private final Optional<CustomPageDatabase> database;

    @Inject
    public CustomPageHttpService(Optional<CustomPageDatabase> database) {
        this.database = database;
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
    @JsonRequest
    @ProtectEndpoint(writeOperation = true)
    @ApiOperation(value = "Save Report", authorizations = @Authorization(value = "read_key"),
            response = SuccessMessage.class, request = CustomPageDatabase.Page.class)
    public SuccessMessage save(
            @Named("user_id") UIPermissionParameterProvider.Project project, @BodyParam CustomPageDatabase.Page report) {
        database.get().save(project.userId, project.project, report);
        return SuccessMessage.success();
    }

    @Path("/delete")
    @ApiOperation(value = "Delete Report", authorizations = @Authorization(value = "read_key"))
    @JsonRequest
    @ProtectEndpoint(writeOperation = true)
    public SuccessMessage delete(@com.google.inject.name.Named("user_id") UIPermissionParameterProvider.Project project,
                                 @ApiParam("name") String name) {
        if (!database.isPresent()) {
            throw new RakamException("Custom page feature is not implemented", NOT_IMPLEMENTED);
        }

        database.get().delete(project.project, name);
        return SuccessMessage.success();
    }

    @Path("/check")
    @ApiOperation(value = "Check feature exists", authorizations = @Authorization(value = "read_key"))
    @GET
    public boolean check() {
        return !database.isPresent();
    }

    @Path("/get")
    @ApiOperation(value = "Get Report", authorizations = @Authorization(value = "read_key"))
    @JsonRequest
    public Map<String, String> get(@com.google.inject.name.Named("user_id") UIPermissionParameterProvider.Project project,
                                   @ApiParam("slug") String slug) {
        if (!database.isPresent()) {
            throw new RakamException("Custom page feature is not implemented", NOT_IMPLEMENTED);
        }
        return database.get().get(project.project, slug);
    }

    @Path("/display/*")
    @GET
    public void display(RakamHttpRequest request) {
        if (!database.isPresent()) {
            throw new RakamException("Custom page feature is not implemented", NOT_IMPLEMENTED);
        }
        String path = request.path().substring(21);
        String[] projectCustomPage = path.split("/", 3);
        byte[] bytes;
        try {
            InputStream file = database.get().getFile(Integer.parseInt(projectCustomPage[0]), projectCustomPage[1], projectCustomPage[2]);
            if (file == null) {
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
    public List<CustomPageDatabase.Page> list(@Named("user_id") UIPermissionParameterProvider.Project project) {
        if (!database.isPresent()) {
            return null;
        }
        return database.get().list(project.project);
    }
}
