package org.rakam.ui;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import org.rakam.collection.FieldType;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.net.HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN;
import static com.google.common.net.HttpHeaders.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static org.rakam.ui.ScheduledTaskUIHttpService.getResourceFiles;

@IgnoreApi
@Path("/ui/webhook")
@Api(value = "/ui/webhook")
public class WebHookUIHttpService
        extends HttpService {
    @GET
    @ApiOperation(value = "List webhooks", response = Integer.class)
    @Path("/list")
    public List<UIWebHook> list() {
        List<String> resourceFiles;
        try {
            resourceFiles = getResourceFiles("webhook");
        } catch (IOException e) {
            throw new RakamException("Unable to read files", INTERNAL_SERVER_ERROR);
        }

        return resourceFiles.stream().flatMap(e -> {
            UIWebHook resource;
            try {
                URL config = getClass().getResource("/webhook/" + e + "/config.json");
                byte[] script = ByteStreams.toByteArray(getClass().getResource("/webhook/" + e + "/script.js").openStream());
                String test = null;
                try {
                    test = new String(ByteStreams.toByteArray(getClass()
                            .getResource("/webhook/" + e + "/test.txt").openStream()),
                            StandardCharsets.UTF_8);
                } catch (Exception e1) {
                    test = null;
                }
                resource = JsonHelper.read(ByteStreams.toByteArray(config.openStream()), UIWebHook.class);
                resource.script = new String(script, StandardCharsets.UTF_8);
                resource.testCode = test;
                resource.image = "/ui/webhook/image/" + e;
            } catch (IOException ex) {
                return Stream.of();
            }

            return Stream.of(resource);
        }).collect(Collectors.toList());
    }

    @GET
    @ApiOperation(value = "Show webhook images", response = Integer.class)
    @Path("/image/*")
    public void image(RakamHttpRequest request) {
        String substring = request.path().substring("/ui/webhook/image".length() + 1);
        if (!substring.matches("^[A-Za-z0-9-]+$")) {
            throw new RakamException(FORBIDDEN);
        }

        URL resource = getClass().getResource("/webhook/" + substring + "/image.png");
        if (resource == null) {
            throw new RakamException(NOT_FOUND);
        }
        byte[] script;
        try {
            script = ByteStreams.toByteArray(resource.openStream());
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        DefaultFullHttpResponse resp = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(script));
        resp.headers().add(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        resp.headers().add(CACHE_CONTROL, "private, max-age=86400");
        HttpHeaders.setContentLength(resp, script.length);
        resp.headers().set(CONTENT_TYPE, "image/png");
        request.response(resp).end();
    }

    public static class Parameter {
        public final FieldType type;
        public final String placeholder;
        public final String description;
        public final Object value;
        public final boolean hidden;
        public final boolean required;

        @JsonCreator
        public Parameter(
                @ApiParam("type") FieldType type,
                @ApiParam("placeholder") String placeholder,
                @ApiParam("description") String description,
                @ApiParam(value = "value", required = false) Object value,
                @ApiParam(value = "hidden", required = false) Boolean hidden,
                @ApiParam(value = "required", required = false) Boolean required) {
            this.type = type;
            this.placeholder = placeholder;
            this.description = description;
            this.value = value;
            this.hidden = Boolean.TRUE.equals(hidden);
            this.required = Boolean.TRUE.equals(required);
        }
    }

    public static class UIWebHook {
        public final String name;
        public final String description;
        public final Map<String, Parameter> parameters;
        public String image;
        public String script;
        public String testCode;

        @JsonCreator
        public UIWebHook(@ApiParam("name") String name,
                         @ApiParam(value = "image", required = false) String image,
                         @ApiParam("description") String description,
                         @ApiParam(value = "script", required = false) String script,
                         @ApiParam(value = "testCode", required = false) String testCode,
                         @ApiParam("parameters") Map<String, Parameter> parameters) {
            this.name = name;
            this.image = image;
            this.description = description;
            this.testCode = testCode;
            this.script = script;
            this.parameters = parameters;
        }
    }
}
