package org.rakam;

import com.facebook.presto.hive.$internal.com.google.common.base.Throwables;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.InvalidConfigurationException;
import org.rakam.analysis.ProjectNotExistsException;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEventListener;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/07/15 12:29.
 */
@AutoService(RakamModule.class)
@ConditionalModule(config = "recipe")
public class RecipeModule extends RakamModule {

    private final QueryMetadataStore queryMetadataStore;
    private final Metastore metastore;

    @Inject
    public RecipeModule(Metastore metastore, QueryMetadataStore queryMetadataStore) {
        this.metastore = metastore;
        this.queryMetadataStore = queryMetadataStore;
    }

    @Override
    protected void setup(Binder binder) {
        String recipeConfig = getConfig("recipe");

        URI recipeUri;
        try {
            recipeUri = new URI(recipeConfig);
        } catch (URISyntaxException e) {
            throw Throwables.propagate(new InvalidConfigurationException("The value of 'recipe' config must be an URI."));
        }

        InputStream stream;
        switch (recipeUri.getScheme()) {
            case "file":
                try {
                    stream = new FileInputStream(recipeUri.getPath());
                } catch (FileNotFoundException e) {
                    throw Throwables.propagate(e);
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
            binder.addError("'recipe' file %s couldn't parsed: %s", recipeConfig, e.getMessage());
            return;
        }

        binder.bind(Recipe.class).toInstance(recipe);

        switch (recipe.getStrategy()) {
            case DEFAULT:
                Multibinder<SystemEventListener> events = Multibinder.newSetBinder(binder, SystemEventListener.class);
                events.addBinding().to(RecipeLoader.class).in(Scopes.SINGLETON);
                break;
            case SPECIFIC:
                new RecipeLoader(recipe, metastore, queryMetadataStore).onCreateProject(recipe.getProject());
                break;
            default:
                throw new IllegalStateException();

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

        private final Recipe recipe;
        private final Metastore metastore;
        private final QueryMetadataStore queryMetadataStore;

        @Inject
        public RecipeLoader(Recipe recipe, Metastore metastore, QueryMetadataStore queryMetadataStore) {
            this.recipe = recipe;
            this.metastore = metastore;
            this.queryMetadataStore = queryMetadataStore;
        }

        @Override
        public void onCreateProject(String project) {
            recipe.getCollections().forEach((collectionName, schema) -> {
                metastore.createProject(project);
                try {
                    metastore.createOrGetCollectionField(project, collectionName, schema.getColumns());
                } catch (ProjectNotExistsException e) {
                    throw Throwables.propagate(e);
                }

                recipe.getContinuousQueryBuilders().stream()
                        .map(builder -> builder.createContinuousQuery(project))
                        .forEach(continuousQuery -> {
                            queryMetadataStore.createContinuousQuery(continuousQuery);
                        });

                recipe.getMaterializedViewBuilders().stream()
                        .map(builder -> builder.createMaterializedView(project))
                        .forEach(continuousQuery -> {
                            queryMetadataStore.createMaterializedView(continuousQuery);
                        });
            });
        }

    }


}
