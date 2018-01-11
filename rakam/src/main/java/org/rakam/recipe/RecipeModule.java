package org.rakam.recipe;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.auto.service.AutoService;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.Config;
import io.airlift.configuration.InvalidConfigurationException;
import io.swagger.models.Tag;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.metadata.SchemaChecker;
import org.rakam.config.MetadataConfig;
import org.rakam.plugin.InjectionHook;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEvents.ProjectCreatedEvent;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.SwaggerJacksonAnnotationIntrospector;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

@AutoService(RakamModule.class)
public class RecipeModule extends RakamModule {

    @Override
    protected void setup(Binder binder) {
        RecipeConfig recipes = buildConfigObject(RecipeConfig.class);


        Multibinder.newSetBinder(binder, Tag.class).addBinding()
                .toInstance(new Tag().name("recipe").description("Recipe")
                        .externalDocs(MetadataConfig.centralDocs));

        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(RecipeHttpService.class).in(Scopes.SINGLETON);

        binder.bind(RecipeHandler.class).in(Scopes.SINGLETON);

        if (recipes.getRecipes() == null) {
            return;
        }

        boolean set_default = false;
        for (String recipeConfig : recipes.getRecipes()) {
            URI recipeUri;
            try {
                recipeUri = new URI(recipeConfig);
            } catch (URISyntaxException e) {
                throw Throwables.propagate(new InvalidConfigurationException("The value of 'recipe' config must be an URI."));
            }

            String path = "/" + recipeUri.getHost() + recipeUri.getPath();
            InputStream stream;
            switch (recipeUri.getScheme()) {
                case "file":
                    try {
                        stream = new FileInputStream(path);
                    } catch (FileNotFoundException e) {
                        throw Throwables.propagate(e);
                    }
                    break;
                case "resources":
                    stream = getClass().getResourceAsStream(path);
                    if (stream == null) {
                        throw new IllegalArgumentException("Recipe file couldn't found: " + recipeUri);
                    }
                    break;
                case "http":
                case "https":
                    try {
                        stream = recipeUri.toURL().openStream();
                    } catch (IOException e) {
                        binder.addError("The value of 'recipe' property '%s' is not an URL");
                        return;
                    }
                    break;
                default:
                    binder.addError("The scheme of 'recipe' %s is not valid", recipeConfig, recipeUri.getScheme());
                    return;
            }

            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.registerModule(new SimpleModule() {
                @Override
                public void setupModule(SetupContext context) {
                    context.insertAnnotationIntrospector(new SwaggerJacksonAnnotationIntrospector());
                }
            });
            Recipe recipe;
            try {
                recipe = mapper.readValue(stream, Recipe.class);
            } catch (IOException e) {
                binder.addError("'recipes' file %s couldn't parsed: %s", recipeConfig, e.getMessage());
                return;
            }

            switch (recipe.getStrategy()) {
                case DEFAULT:
                    if (set_default) {
                        binder.addError("Only one recipe can use DEFAULT strategy.");
                        return;
                    }
                    binder.bind(Recipe.class).toInstance(recipe);
                    binder.bind(RecipeLoader.class).asEagerSingleton();
                    set_default = true;
                    break;
                case SPECIFIC:
//                    Multibinder<InjectionHook> hooks = Multibinder.newSetBinder(binder, InjectionHook.class);
//                    hooks.addBinding().toInstance(new RecipeLoaderSingle(recipe));
                    break;
                default:
                    throw new IllegalStateException();

            }
        }
    }

    @Override
    public String name() {
        return "Recipe module";
    }

    @Override
    public String description() {
        return null;
    }

    private static class RecipeLoader {
        private final Recipe recipe;
        private final RecipeHandler installer;

        @Inject
        public RecipeLoader(Recipe recipe, Metastore metastore,
                            ConfigManager configManager,
                            SchemaChecker schemaChecker,
                            MaterializedViewService materializedViewService) {
            this.recipe = recipe;
            this.installer = new RecipeHandler(metastore, configManager, schemaChecker, materializedViewService);
        }

        @Subscribe
        public void onCreateProject(ProjectCreatedEvent event) {
            installer.install(recipe, event.project, false);
        }

    }

    public static class RecipeConfig {

        private List<String> recipes;

        public List<String> getRecipes() {
            return recipes;
        }

        @Config("recipes")
        public RecipeConfig setRecipes(String recipes) {
            this.recipes = ImmutableList.copyOf(Splitter.on(',')
                    .omitEmptyStrings().trimResults()
                    .split(recipes));
            return this;
        }
    }

    public static class RecipeLoaderSingle implements InjectionHook {

        private final Recipe recipe;
        private Metastore metastore;

        public RecipeLoaderSingle(Recipe recipe) {
            this.recipe = recipe;
        }

        @Inject
        public void setMetastore(Metastore metastore) {
            this.metastore = metastore;
        }

        @Override
        public void call() {
            metastore.createProject(null);
        }
    }
}
