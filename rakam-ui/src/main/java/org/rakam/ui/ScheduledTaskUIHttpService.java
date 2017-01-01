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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.net.HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN;
import static com.google.common.net.HttpHeaders.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;

@IgnoreApi
@Path("/ui/scheduled-task")
@Api(value = "/ui/scheduled-task")
public class ScheduledTaskUIHttpService
        extends HttpService
{
    @GET
    @ApiOperation(value = "List scheduled job", response = Integer.class)
    @Path("/list")
    public List<ScheduledTask> list()
    {
        List<String> resourceFiles;
        try {
            resourceFiles = getResourceFiles("scheduled-task");
        }
        catch (IOException e) {
            throw new RakamException("Unable to read files", INTERNAL_SERVER_ERROR);
        }

        return resourceFiles.stream().flatMap(e -> {
            ScheduledTask resource;
            try {
                URL config = getClass().getResource("/scheduled-task/" + e + "/config.json");
                byte[] script = ByteStreams.toByteArray(getClass().getResource("/scheduled-task/" + e + "/script.js").openStream());
                resource = JsonHelper.read(ByteStreams.toByteArray(config.openStream()), ScheduledTask.class);
                resource.script = new String(script, StandardCharsets.UTF_8);
                resource.image = "/ui/scheduled-task/image/" + e;
            }
            catch (IOException ex) {
                return Stream.of();
            }

            return Stream.of(resource);
        }).collect(Collectors.toList());
    }

    @GET
    @ApiOperation(value = "List scheduled job", response = Integer.class)
    @Path("/image/*")
    public void image(RakamHttpRequest request)
    {
        String substring = request.path().substring(25);
        if (!substring.matches("^[A-Za-z0-9-]+$")) {
            throw new RakamException(FORBIDDEN);
        }

        URL resource = getClass().getResource("/scheduled-task/" + substring + "/image.png");
        if (resource == null) {
            throw new RakamException(NOT_FOUND);
        }
        byte[] script;
        try {
            script = ByteStreams.toByteArray(resource.openStream());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        DefaultFullHttpResponse resp = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(script));
        resp.headers().add(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        resp.headers().add(CACHE_CONTROL, "private, max-age=86400");
        HttpHeaders.setContentLength(resp, script.length);
        resp.headers().set(CONTENT_TYPE, "image/png");
        request.response(resp).end();
    }

    private List<String> getResourceFiles(String path)
            throws IOException
    {
        List<String> filenames = new ArrayList<>();

        try (InputStream in = getResourceAsStream(path); BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
            String resource;

            while ((resource = br.readLine()) != null) {
                filenames.add(resource);
            }
        }

        return filenames;
    }

    private InputStream getResourceAsStream(String resource)
    {
        final InputStream in = getContextClassLoader().getResourceAsStream(resource);
        return in == null ? getClass().getResourceAsStream(resource) : in;
    }

    private ClassLoader getContextClassLoader()
    {
        return Thread.currentThread().getContextClassLoader();
    }

    public static class Parameter
    {
        public final FieldType type;
        public final Object value;
        public final String placeholder;
        public final String description;
        public final List<Choice> choices;

        @JsonCreator
        public Parameter(
                @ApiParam("type") FieldType type,
                @ApiParam(value = "value", required = false) Object value,
                @ApiParam(value = "placeholder", required = false) String placeholder,
                @ApiParam(value = "choices", required = false) List<Choice> choices,
                @ApiParam(value = "description", required = false) String description)
        {
            this.type = type;
            this.value = value;
            this.placeholder = placeholder;
            this.choices = choices;
            this.description = description;
        }

        public static class Choice
        {
            public final String key;
            public final String value;

            @JsonCreator
            public Choice(@ApiParam("key") String key, @ApiParam("value") String value)
            {
                this.key = key;
                this.value = value;
            }
        }
    }

    public static class ScheduledTask
    {
        public final String name;
        public External external;
        public String image;
        public final String description;
        public String script;
        public Duration defaultDuration;
        public final Map<String, Parameter> parameters;

        @JsonCreator
        public ScheduledTask(
                @ApiParam("name") String name,
                @ApiParam(value = "image", required = false) String image,
                @ApiParam(value = "externalUrl", required = false) External external,
                @ApiParam(value = "defaultDuration", required = false) Duration defaultDuration,
                @ApiParam(value = "description", required = false) String description,
                @ApiParam(value = "script", required = false) String script,
                @ApiParam(value = "parameters", required = false) Map<String, Parameter> parameters)
        {
            this.name = name;
            this.image = image;
            this.external = external;
            this.defaultDuration = defaultDuration;
            this.description = description;
            this.script = script;
            this.parameters = parameters;
        }

        public static class External {
            public final URL url;
            public final String name;

            @JsonCreator
            public External(@ApiParam("url") URL url, @ApiParam("name") String name) {
                this.url = url;
                this.name = name;
            }
        }
    }
}
