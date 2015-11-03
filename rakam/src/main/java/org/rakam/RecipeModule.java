package org.rakam;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEvents;
import org.rakam.ui.JDBCReportMetadata;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

@AutoService(RakamModule.class)
@ConditionalModule(config = "recipes")
public class RecipeModule extends RakamModule {

    @Override
    protected void setup(Binder binder) {
        RecipeConfig recipes = buildConfigObject(RecipeConfig.class);

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
                    if(stream == null) {
                        throw new IllegalArgumentException("Recipe file couldn't found: "+recipeUri);
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
            Recipe recipe;
            try {
                recipe = mapper.readValue(stream, Recipe.class);
            } catch (IOException e) {
                binder.addError("'recipes' file %s couldn't parsed: %s", recipeConfig, e.getMessage());
                return;
            }

            switch (recipe.getStrategy()) {
                case DEFAULT:
                    if(set_default) {
                        binder.addError("Only one recipe can use DEFAULT strategy.");
                        return;
                    }
                    binder.bind(Recipe.class).toInstance(recipe);
                    binder.bind(RecipeLoader.class).asEagerSingleton();
                    binder.bind(RecipeHandler.class).in(Scopes.SINGLETON);
                    set_default = true;
                    break;
                case SPECIFIC:
                    Multibinder<InjectionHook> hooks = Multibinder.newSetBinder(binder, InjectionHook.class);
                    hooks.addBinding().toInstance(new RecipeLoaderSingle(recipe));
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
        public RecipeLoader(Recipe recipe, Metastore metastore, ContinuousQueryService continuousQueryService, MaterializedViewService materializedViewService, JDBCReportMetadata reportMetadata) {
            this.recipe = recipe;
            this.installer = new RecipeHandler(metastore, continuousQueryService, materializedViewService, reportMetadata);
        }

        @Subscribe
        public void onCreateProject(SystemEvents.ProjectCreatedEvent event) {
            installer.install(recipe, event.project);
        }

    }

    public static class RecipeConfig {

        private List<String> recipes;

        @Config("recipes")
        public RecipeConfig setRecipes(String recipes) {
            this.recipes = ImmutableList.copyOf(Splitter.on(',')
                    .omitEmptyStrings().trimResults()
                    .split(recipes));
            return this;
        }

        public List<String> getRecipes() {
            return recipes;
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
            metastore.createProject(recipe.getProject());
        }
    }
}
