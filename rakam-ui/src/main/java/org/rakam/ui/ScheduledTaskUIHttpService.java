package org.rakam.ui;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.CharMatcher;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import org.rakam.plugin.Parameter;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.*;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.net.HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN;
import static com.google.common.net.HttpHeaders.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;

@IgnoreApi
@Path("/ui/scheduled-task")
@Api(value = "/ui/scheduled-task")
public class ScheduledTaskUIHttpService
        extends HttpService {
    public synchronized static final List<String> getResourceFiles(String path)
            throws IOException {
        FileSystem fileSystem = null;

        try {
            URI uri = ScheduledTaskUIHttpService.class.getResource("/" + path).toURI();

            java.nio.file.Path myPath;
            try {
                if (uri.getScheme().equals("jar")) {
                    fileSystem = FileSystems.newFileSystem(uri, Collections.emptyMap());
                    myPath = fileSystem.getPath("/" + path);
                } else {
                    myPath = Paths.get(uri);
                }
                return Files.walk(myPath, 1).flatMap(next -> {
                    if (next.equals(myPath)) {
                        return Stream.of();
                    }
                    return Stream.of(CharMatcher.is('/').trimFrom(next.getFileName().toString()));
                }).collect(Collectors.toList());
            } finally {
                if (fileSystem != null) {
                    fileSystem.close();
                }
            }
        } catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        } finally {
            if (fileSystem != null) {
                fileSystem.close();
            }
        }
    }

    @GET
    @ApiOperation(value = "List scheduled job")
    @Path("/list")
    public List<ScheduledTask> list() {
        List<String> resourceFiles;
        try {
            resourceFiles = getResourceFiles("scheduled-task");
        } catch (IOException e) {
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
                resource.slug = e;
            } catch (IOException ex) {
                return Stream.of();
            }

            return Stream.of(resource);
        }).collect(Collectors.toList());
    }

    @ApiOperation(value = "Get scheduled job")
    @JsonRequest
    @Path("/get")
    public ScheduledTask get(@ApiParam("name") String name) {
        List<String> resourceFiles;
        try {
            resourceFiles = getResourceFiles("scheduled-task");
        } catch (IOException e) {
            throw new RakamException("Unable to read files", INTERNAL_SERVER_ERROR);
        }

        if (!resourceFiles.contains(name)) {
            throw new RakamException(NOT_FOUND);
        }

        ScheduledTask resource;
        try {
            URL config = getClass().getResource("/scheduled-task/" + name + "/config.json");
            byte[] script = ByteStreams.toByteArray(getClass().getResource("/scheduled-task/" + name + "/script.js").openStream());
            resource = JsonHelper.read(ByteStreams.toByteArray(config.openStream()), ScheduledTask.class);
            resource.script = new String(script, StandardCharsets.UTF_8);
            resource.image = "/ui/scheduled-task/image/" + name;
            resource.slug = name;
            return resource;
        } catch (IOException ex) {
            throw new RakamException(NOT_FOUND);
        }
    }

    @GET
    @ApiOperation(value = "List scheduled job", response = byte[].class)
    @Path("/image/*")
    public void image(RakamHttpRequest request) {
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

    public static class ScheduledTask {
        public final String name;
        public final String recipe;
        public final String description;
        public final Map<String, Parameter> parameters;
        public String slug;
        public External external;
        public String image;
        public String script;
        public Duration defaultDuration;

        @JsonCreator
        public ScheduledTask(
                @ApiParam(value = "slug", required = false) String slug,
                @ApiParam("name") String name,
                @ApiParam(value = "image", required = false) String image,
                @ApiParam(value = "recipe", required = false) String recipe,
                @ApiParam(value = "externalUrl", required = false) External external,
                @ApiParam(value = "defaultDuration", required = false) Duration defaultDuration,
                @ApiParam(value = "description", required = false) String description,
                @ApiParam(value = "script", required = false) String script,
                @ApiParam(value = "parameters", required = false) Map<String, Parameter> parameters) {
            this.slug = slug;
            this.name = name;
            this.image = image;
            this.recipe = recipe;
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
