package org.rakam.ui;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Throwables;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.io.ByteStreams.toByteArray;
import static com.google.common.net.HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN;
import static com.google.common.net.HttpHeaders.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static org.rakam.ui.ScheduledTaskUIHttpService.getResourceFiles;

@IgnoreApi
@Path("/ui/custom-event-mapper")
@Api(value = "/ui/custom-event-mapper")
public class CustomEventMapperUIHttpService
        extends HttpService {
    @GET
    @ApiOperation(value = "List custom event mapper", response = Integer.class)
    @Path("/list")
    public List<UIEventMapper> list() {
        List<String> resourceFiles;
        try {
            resourceFiles = getResourceFiles("custom-event-mapper");
        } catch (IOException e) {
            throw new RakamException("Unable to read files", INTERNAL_SERVER_ERROR);
        }

        return resourceFiles.stream().flatMap(e -> {
            UIEventMapper resource;
            try {
                URL config = getClass().getResource("/custom-event-mapper/" + e + "/config.json");
                byte[] script = toByteArray(getClass().getResource("/custom-event-mapper/" + e + "/script.js").openStream());
                resource = JsonHelper.read(toByteArray(config.openStream()), UIEventMapper.class);
                resource.script = new String(script, UTF_8);
                resource.image = "/ui/custom-event-mapper/image/" + e;
            } catch (IOException ex) {
                return Stream.of();
            }

            return Stream.of(resource);
        }).collect(Collectors.toList());
    }

    @GET
    @ApiOperation(value = "List custom event mappers", response = Integer.class)
    @Path("/image/*")
    public void image(RakamHttpRequest request) {
        String substring = request.path().substring("/ui/custom-event-mapper/image".length() + 1);
        if (!substring.matches("^[A-Za-z0-9-]+$")) {
            throw new RakamException(FORBIDDEN);
        }

        URL resource = getClass().getResource("/custom-event-mapper/" + substring + "/image.png");
        if (resource == null) {
            throw new RakamException(NOT_FOUND);
        }
        byte[] script;
        try {
            script = toByteArray(resource.openStream());
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

        @JsonCreator
        public Parameter(
                @ApiParam("type") FieldType type,
                @ApiParam("placeholder") String placeholder,
                @ApiParam("description") String description,
                @ApiParam(value = "value", required = false) Object value) {
            this.type = type;
            this.placeholder = placeholder;
            this.description = description;
            this.value = value;
        }
    }

    public static class UIEventMapper {
        public final String name;
        public final String description;
        public final Map<String, Parameter> parameters;
        public String image;
        public String script;

        @JsonCreator
        public UIEventMapper(@ApiParam("name") String name,
                             @ApiParam(value = "image", required = false) String image,
                             @ApiParam(value = "description", required = false) String description,
                             @ApiParam(value = "code", required = false) String code,
                             @ApiParam("parameters") Map<String, Parameter> parameters) {
            this.name = name;
            this.image = image;
            this.description = description;
            this.script = code;
            this.parameters = parameters;
        }
    }
}
