package org.rakam;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.auto.service.AutoService;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.Config;
import io.airlift.configuration.InvalidConfigurationException;
import io.airlift.log.Logger;
import org.rakam.analysis.ProjectNotExistsException;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEventListener;
import org.rakam.util.ProjectCollection;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

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
                    Multibinder<SystemEventListener> events = Multibinder.newSetBinder(binder, SystemEventListener.class);
                    events.addBinding().to(RecipeLoader.class).in(Scopes.SINGLETON);
                    set_default = true;
                    break;
                case SPECIFIC:
                    Multibinder<InjectionHook> hooks = Multibinder.newSetBinder(binder, InjectionHook.class);
                    hooks.addBinding().toInstance(new RecipeLoaderSingle(recipe));
//                    Multibinder<InjectionHook> hooks = Multibinder.newSetBinder(binder, InjectionHook.class);
//                    hooks.addBinding().toInstance(new RecipeLoaderSingle(recipe));
//                    new RecipeLoader(recipe)
//                            .onCreateProject(recipe.project());
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

    private static class RecipeLoader implements SystemEventListener {
        private Logger logger = Logger.get(RecipeLoader.class);

        private final Recipe recipe;
        private final Metastore metastore;
        private final QueryMetadataStore queryMetadataStore;
        private final Set<SystemEventListener> listeners;

        @Inject
        public RecipeLoader(Recipe recipe, Metastore metastore, Set<SystemEventListener> listeners, QueryMetadataStore queryMetadataStore) {
            this.recipe = recipe;
            this.metastore = metastore;
            this.queryMetadataStore = queryMetadataStore;
            this.listeners = listeners;
        }

        @Override
        public void onCreateProject(String project) {
            recipe.getCollections().forEach((collectionName, schema) -> {
                metastore.createProject(project);
                try {
                    metastore.createOrGetCollectionField(project, collectionName, schema.getColumns(), new Consumer<ProjectCollection>() {
                        @Override
                        public void accept(ProjectCollection projectCollection) {
                            for (SystemEventListener listener : listeners) {
                                try {
                                    listener.onCreateCollection(projectCollection.project, projectCollection.collection);
                                } catch (Exception e) {
                                    logger.error(e, "Error while processing event listener");
                                }
                            }
                        }
                    });
                } catch (ProjectNotExistsException e) {
                    throw Throwables.propagate(e);
                }

                recipe.getContinuousQueryBuilders().stream()
                        .map(builder -> builder.createContinuousQuery(project))
                        .forEach(continuousQuery ->
                                queryMetadataStore.createContinuousQuery(continuousQuery));

                recipe.getMaterializedViewBuilders().stream()
                        .map(builder -> builder.createMaterializedView(project))
                        .forEach(continuousQuery ->
                                queryMetadataStore.createMaterializedView(continuousQuery));

                recipe.getReports().stream().forEach(reportBuilder ->
                        reportBuilder.createReport(project));
            });
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
        private QueryMetadataStore queryMetadataStore;
        private Set<SystemEventListener> listeners;

        public RecipeLoaderSingle(Recipe recipe) {
            this.recipe = recipe;
        }

        @Inject
        public void setMetastore(Metastore metastore) {
            this.metastore = metastore;
        }

        @Inject
        public void setQueryMetadataStore(QueryMetadataStore queryMetadataStore) {
            this.queryMetadataStore = queryMetadataStore;
        }

        @Inject
        public void setListeners(Set<SystemEventListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void call() {
            new RecipeLoader(recipe, metastore, listeners, queryMetadataStore)
                    .onCreateProject(recipe.getProject());
        }
    }
}
