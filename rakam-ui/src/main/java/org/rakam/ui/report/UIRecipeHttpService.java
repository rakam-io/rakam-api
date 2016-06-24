package org.rakam.ui.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.HeaderParam;
import org.rakam.util.JsonHelper;
import org.rakam.util.SuccessMessage;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.Arrays;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static java.lang.Boolean.TRUE;
import static org.rakam.server.http.HttpServer.returnError;

@Path("/ui/recipe")
@Api(value = "/ui/recipe", nickname = "recipe", description = "Recipe operations", tags = "recipe")
public class UIRecipeHttpService extends HttpService {
    private static ObjectMapper yamlMapper;
    static {
        yamlMapper = new ObjectMapper(new YAMLFactory());
        yamlMapper.registerModule(new JSR310Module());
        yamlMapper.registerModule(new Jdk8Module());
    }

    private final UIRecipeHandler installer;

    @Inject
    public UIRecipeHttpService(UIRecipeHandler installer) {
        this.installer = installer;
    }

    @ApiOperation(value = "Install recipe",
            authorizations = @Authorization(value = "master_key"), response = SuccessMessage.class
    )
    @POST
    @Path("/install")
    public void installUIRecipe(RakamHttpRequest request, @HeaderParam("project") int project) {
        String contentType = request.headers().get(CONTENT_TYPE);
        ExportType exportType = Arrays.stream(ExportType.values())
                .filter(f -> f.contentType.equals(contentType))
                .findAny()
                .orElse(ExportType.YAML);

        boolean override = ImmutableList.of(TRUE.toString()).equals(request.params().get("override"));

        request.bodyHandler(body -> {
            UIRecipe recipe;

            try {
                recipe = exportType.mapper.readValue(body, UIRecipe.class);
            } catch (IOException e) {
                returnError(request, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
                return;
            }

            try {
                installer.install(recipe, project, override);
                request.response(JsonHelper.encode(SuccessMessage.success())).end();
            } catch (Exception e) {
                returnError(request, "Error loading recipe: " + e.getMessage(), HttpResponseStatus.BAD_REQUEST);
            }

        });
    }

    @ApiOperation(value = "Export recipe", request = ExportRequest.class, response = UIRecipe.class,
            authorizations = @Authorization(value = "master_key")
    )
    @GET
    @Path("/export")
    public void exportUIRecipe(@HeaderParam("Accept") String contentType, @HeaderParam("project") int project, RakamHttpRequest request) throws JsonProcessingException {
        request.bodyHandler(s -> {
            UIRecipe export = installer.export(project);

            ExportType exportType = Arrays.stream(ExportType.values())
                    .filter(f -> f.contentType.equals(contentType))
                    .findAny()
                    .orElse(ExportType.YAML);

            ByteBuf buffer;
            try {
                buffer = Unpooled.wrappedBuffer(exportType.mapper.writeValueAsBytes(export));
            } catch (JsonProcessingException e) {
                throw Throwables.propagate(e);
            }

            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, buffer);
            response.headers().add(CONTENT_TYPE, exportType.contentType);
            request.response(response).end();
        });
    }

    public static class ExportRequest {
        public final ExportType type;

        @JsonCreator
        public ExportRequest(@ApiParam(value = "type", required = false) ExportType type) {
            this.type = type;
        }
    }

    public enum ExportType {
        JSON(JsonHelper.getMapper(), "application/json"), YAML(yamlMapper, "application/x-yaml");

        private final ObjectMapper mapper;
        private final String contentType;

        ExportType(ObjectMapper mapper, String contentType) {
            this.mapper = mapper;
            this.contentType = contentType;
        }

        @JsonCreator
        public static ExportType get(String name) {
            return valueOf(name.toUpperCase());
        }
    }
}
