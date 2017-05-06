package org.rakam.recipe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.metadata.SchemaChecker;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.QueryResult;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.String.format;
import static org.rakam.analysis.InternalConfig.USER_TYPE;
import static org.rakam.collection.FieldType.STRING;
import static org.rakam.report.QueryError.create;

public class RecipeHandler
{
    private final Metastore metastore;
    private final ContinuousQueryService continuousQueryService;
    private final MaterializedViewService materializedViewService;
    private final ConfigManager configManager;
    private final SchemaChecker schemaChecker;

    @Inject
    public RecipeHandler(
            Metastore metastore,
            ContinuousQueryService continuousQueryService,
            ConfigManager configManager,
            SchemaChecker schemaChecker,
            MaterializedViewService materializedViewService)
    {
        this.metastore = metastore;
        this.configManager = configManager;
        this.schemaChecker = schemaChecker;
        this.materializedViewService = materializedViewService;
        this.continuousQueryService = continuousQueryService;
    }

    public Recipe export(String project)
    {
        final Map<String, Recipe.CollectionDefinition> collections = metastore.getCollections(project).entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> {
            List<Map<String, Recipe.SchemaFieldInfo>> map = e.getValue().stream()
                    .map(a -> ImmutableMap.of(a.getName(), new Recipe.SchemaFieldInfo(a.getCategory(), a.getType())))
                    .collect(Collectors.toList());
            return new Recipe.CollectionDefinition(map);
        }));
        final List<MaterializedView> materializedViews = materializedViewService.list(project).stream()
                .map(m -> new MaterializedView(m.tableName, m.name, m.query, m.updateInterval, m.incremental, m.realTime, m.options))
                .collect(Collectors.toList());
        final List<ContinuousQuery> continuousQueryBuilders = continuousQueryService.list(project).stream()
                .map(m -> new ContinuousQuery(m.tableName, m.name, m.query, m.partitionKeys, m.options))
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
            List<SchemaField> build = collection.build().stream()
                    .map(e -> {
                        FieldType type;
                        if (e.getName().equals("_user")) {
                            type = configManager.setConfigOnce(project, USER_TYPE.name(), STRING);
                        }
                        else {
                            type = e.getType();
                        }
                        SchemaField schemaField = new SchemaField(e.getName(), type, e.isUnique(), e.getDescriptiveName(), e.getDescription(), e.getCategory());
                        return schemaField;
                    })
                    .collect(Collectors.toList());

            HashSet<SchemaField> schemaFields = schemaChecker.checkNewFields(collectionName, ImmutableSet.copyOf(build));
            List<SchemaField> fields = metastore.getOrCreateCollectionFieldList(project, collectionName, schemaFields);

            List<SchemaField> collisions = build.stream()
                    .filter(f -> fields.stream().anyMatch(field -> field.getName().equals(f.getName()) && !f.getType().equals(field.getType())))
                    .collect(Collectors.toList());

            if (!collisions.isEmpty()) {
                String errMessage = collisions.stream().map(f -> {
                    SchemaField existingField = fields.stream().filter(field -> field.getName().equals(f.getName())).findAny().get();
                    return format("Recipe: [%s : %s], CollectionDefinition: [%s, %s]", f.getName(), f.getType(),
                            existingField.getName(), existingField.getType());
                }).collect(Collectors.joining(", "));
                String message = overrideExisting ? "Overriding collection fields is not possible." : "Collision in collection fields.";
                throw new RakamException(message + " " + errMessage, BAD_REQUEST);
            }
        });

        List<CompletableFuture<QueryResult>> continuousQueries = recipe.getContinuousQueryBuilders().stream()
                .map(continuousQuery -> continuousQueryService.create(project, continuousQuery, false).getResult().handle((res, ex) -> {
                    if (ex != null) {
                        if (ex.getCause() instanceof AlreadyExistsException) {
                            if (overrideExisting) {
                                try {
                                    continuousQueryService.delete(project, continuousQuery.tableName).join();
                                    return continuousQueryService.create(project, continuousQuery, false).getResult().join();
                                }
                                catch (Throwable e) {
                                    return QueryResult.errorResult(
                                            create(format("Error while re-creating materialized view %s: %s",
                                                    continuousQuery.getTableName(), ex.getMessage())));
                                }
                            }
                            else {
                                return QueryResult.errorResult(create(format("Continuous query %s already exists",
                                        continuousQuery.getTableName())));
                            }
                        }

                        return QueryResult.errorResult(
                                create(format("Error while creating materialized view %s: %s",
                                        continuousQuery.getTableName(), ex.getMessage())));
                    }
                    else {
                        return res;
                    }
                })).collect(Collectors.toList());

        List<CompletableFuture<QueryResult>> materializedViews = recipe.getMaterializedViewBuilders().stream()
                .map(materializedView -> materializedViewService.create(project, materializedView).handle((res, ex) -> {
                    if (ex != null) {
                        if (ex instanceof AlreadyExistsException) {
                            if (overrideExisting) {
                                try {
                                    materializedViewService.delete(project, materializedView.tableName);
                                    materializedViewService.create(project, materializedView);
                                    return QueryResult.empty();
                                }
                                catch (Throwable e) {
                                    return QueryResult.errorResult(
                                            create(format("Error while re-creating materialized view %s: %s",
                                                    materializedView.tableName, e.getMessage())));
                                }
                            }
                            else {
                                return QueryResult.errorResult(create(format("Materialized view %s already exists",
                                        materializedView.tableName)));
                            }
                        }
                        return QueryResult.errorResult(
                                create(format("Error while creating materialized view %s: %s",
                                        materializedView.tableName, ex.getMessage())));
                    }
                    else {
                        return QueryResult.empty();
                    }
                })).collect(Collectors.toList());

        CompletableFuture<QueryResult>[] futures = ImmutableList.builder().addAll(continuousQueries)
                .addAll(materializedViews).build().stream()
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();

        String errors = Arrays.stream(futures)
                .map(e -> e.join())
                .filter(e -> e.isFailed())
                .map(e -> e.getError().toString())
                .collect(Collectors.joining("\n"));

        if (!errors.isEmpty()) {
            throw new RakamException(errors, INTERNAL_SERVER_ERROR);
        }
    }
}
