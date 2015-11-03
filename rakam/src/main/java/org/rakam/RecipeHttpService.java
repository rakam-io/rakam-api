package org.rakam;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.inject.Inject;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.ParamBody;
import org.rakam.util.JsonResponse;

import javax.ws.rs.Path;

@Path("/recipe")
@Api(value = "/recipe", description = "Recipe operations", tags = "recipe")
public class RecipeHttpService {
    private ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    private final RecipeHandler installer;

    @Inject
    public RecipeHttpService(RecipeHandler installer) {
        this.installer = installer;
    }

    @ApiOperation(value = "Install recipe",
            authorizations = @Authorization(value = "master_key")
    )
    @Path("/install")
    public JsonResponse install(@ParamBody Recipe recipe) {
        try {
            installer.install(recipe);
        } catch (Exception e) {
            return JsonResponse.error("Error loading recipe: "+e.getMessage());
        }
        return JsonResponse.success();
    }

    @ApiOperation(value = "Export recipe",
            authorizations = @Authorization(value = "master_key")
    )
    @Path("/install")
    public Recipe export(@ApiParam("project") String project) {
        return installer.export(project);
    }
}
