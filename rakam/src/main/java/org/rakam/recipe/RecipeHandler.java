package org.rakam.recipe;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

public class RecipeHandler
{
    private final Metastore metastore;
    private final ContinuousQueryService continuousQueryService;
    private final MaterializedViewService materializedViewService;

    @Inject
    public RecipeHandler(Metastore metastore, ContinuousQueryService continuousQueryService,
            MaterializedViewService materializedViewService)
    {
        this.metastore = metastore;
        this.materializedViewService = materializedViewService;
        this.continuousQueryService = continuousQueryService;
    }

    public Recipe export(String project)
    {
        final Map<String, Recipe.Collection> collections = metastore.getCollections(project).entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> {
            List<Map<String, Recipe.SchemaFieldInfo>> map = e.getValue().stream()
                    .map(a -> ImmutableMap.of(a.getName(), new Recipe.SchemaFieldInfo(a.getCategory(), a.getType())))
                    .collect(Collectors.toList());
            return new Recipe.Collection(map);
        }));
        final List<MaterializedView> materializedViews = materializedViewService.list(project).stream()
                .map(m -> new MaterializedView(m.tableName, m.query, m.updateInterval, m.incremental, m.options))
                .collect(Collectors.toList());
        final List<ContinuousQuery> continuousQueryBuilders = continuousQueryService.list(project).stream()
                .map(m -> new ContinuousQuery(m.tableName, m.query, m.partitionKeys, m.options))
                .collect(Collectors.toList());

        return new Recipe(Recipe.Strategy.SPECIFIC, collections, materializedViews,
                continuousQueryBuilders);
    }

    public void install(Recipe recipe, String project, boolean overrideExisting)
    {
        installInternal(recipe, project, overrideExisting);
    }

    public void install(String project, Recipe recipe, boolean overrideExisting)
    {
        installInternal(recipe, project, overrideExisting);
    }

    public void installInternal(Recipe recipe, String project, boolean overrideExisting)
    {
        recipe.getCollections().forEach((collectionName, collection) -> {
            List<SchemaField> build = collection.build();
            List<SchemaField> fields = metastore.getOrCreateCollectionFieldList(project, collectionName,
                    ImmutableSet.copyOf(build));
            List<SchemaField> collisions = build.stream()
                    .filter(f -> fields.stream().anyMatch(field -> field.getName().equals(f.getName()) && !f.getType().equals(field.getType())))
                    .collect(Collectors.toList());

            if (!collisions.isEmpty()) {
                String errMessage = collisions.stream().map(f -> {
                    SchemaField existingField = fields.stream().filter(field -> field.getName().equals(f.getName())).findAny().get();
                    return String.format("Recipe: [%s : %s], Collection: [%s, %s]", f.getName(), f.getType(),
                            existingField.getName(), existingField.getType());
                }).collect(Collectors.joining(", "));
                String message = overrideExisting ? "Overriding collection fields is not possible." : "Collision in collection fields.";
                throw new RakamException(message + " " + errMessage, BAD_REQUEST);
            }
        });

        recipe.getContinuousQueryBuilders().stream()
                .forEach(continuousQuery -> continuousQueryService.create(project, continuousQuery, false).getResult().whenComplete((res, ex) -> {
                    if (ex != null) {
                        if (ex instanceof AlreadyExistsException) {
                            if (overrideExisting) {
                                continuousQueryService.delete(project, continuousQuery.tableName);
                                continuousQueryService.create(project, continuousQuery, false);
                            }
                            else {
                                throw Throwables.propagate(ex);
                            }
                        }
                        throw Throwables.propagate(ex);
                    }
                }));

        recipe.getMaterializedViewBuilders().stream()
                .forEach(materializedView -> materializedViewService.create(project, materializedView).whenComplete((res, ex) -> {
                    if (ex != null) {
                        if (ex instanceof AlreadyExistsException) {
                            if (overrideExisting) {
                                materializedViewService.delete(project, materializedView.tableName);
                                materializedViewService.create(project, materializedView);
                            }
                            else {
                                throw Throwables.propagate(ex);
                            }
                        }
                        throw Throwables.propagate(ex);
                    }
                }));
    }
}
