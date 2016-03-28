package org.rakam.recipe;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.rakam.util.AlreadyExistsException;
import org.rakam.collection.SchemaField;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.ui.page.CustomPageDatabase;
import org.rakam.ui.customreport.JDBCCustomReportMetadata;
import org.rakam.ui.JDBCReportMetadata;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

public class RecipeHandler {
    private final Metastore metastore;
    private final ContinuousQueryService continuousQueryService;
    private final MaterializedViewService materializedViewService;
    private final JDBCReportMetadata reportMetadata;
    private final JDBCCustomReportMetadata customReportMetadata;
    private final CustomPageDatabase customPageDatabase;

    @Inject
    public RecipeHandler(Metastore metastore, ContinuousQueryService continuousQueryService,
                         MaterializedViewService materializedViewService,
                         JDBCCustomReportMetadata customReportMetadata,
                         CustomPageDatabase customPageDatabase,
                         JDBCReportMetadata reportMetadata) {
        this.metastore = metastore;
        this.materializedViewService = materializedViewService;
        this.continuousQueryService = continuousQueryService;
        this.customReportMetadata = customReportMetadata;
        this.customPageDatabase = customPageDatabase;
        this.reportMetadata = reportMetadata;
    }

    public Recipe export(String project) {
        final Map<String, Recipe.Collection> collections = metastore.getCollections(project).entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> {
            List<Map<String, Recipe.SchemaFieldInfo>> map = e.getValue().stream()
                    .map(a -> ImmutableMap.of(a.getName(), new Recipe.SchemaFieldInfo(a.getCategory(), a.getType())))
                    .collect(Collectors.toList());
            return new Recipe.Collection(map);
        }));
        final List<Recipe.MaterializedViewBuilder> materializedViews = materializedViewService.list(project).stream()
                .map(m -> new Recipe.MaterializedViewBuilder(m.name, m.tableName, m.query, m.updateInterval, m.incrementalField))
                .collect(Collectors.toList());
        final List<Recipe.ContinuousQueryBuilder> continuousQueryBuilders = continuousQueryService.list(project).stream()
                .map(m -> new Recipe.ContinuousQueryBuilder(m.name, m.tableName, m.query, m.partitionKeys, m.options))
                .collect(Collectors.toList());

        final List<Recipe.ReportBuilder> reports = reportMetadata
                .getReports(null, project).stream()
                .map(r -> new Recipe.ReportBuilder(r.slug, r.name, r.category, r.query, r.options, r.shared))
                .collect(Collectors.toList());

        final List<Recipe.CustomReportBuilder> customReports = customReportMetadata
                .list(project).entrySet().stream().flatMap(a -> a.getValue().stream())
                .map(r -> new Recipe.CustomReportBuilder(r.reportType, r.name, r.data))
                .collect(Collectors.toList());

        final List<Recipe.CustomPageBuilder> customPages = customPageDatabase
                .list(project).stream()
                .map(r -> new Recipe.CustomPageBuilder(r.name, r.slug, r.category, customPageDatabase.get(r.project(), r.slug)))
                .collect(Collectors.toList());

        return new Recipe(Recipe.Strategy.SPECIFIC, project, collections, materializedViews,
                continuousQueryBuilders, customReports, customPages, reports);
    }

    public void install(Recipe recipe, String project, boolean overrideExisting) {
        installInternal(recipe, project, overrideExisting);
    }

    public void install(Recipe recipe, boolean overrideExisting) {
        if(recipe.getProject() != null) {
            installInternal(recipe, recipe.getProject(), overrideExisting);
        } else {
            throw new IllegalArgumentException("project is null");
        }
    }

    public void installInternal(Recipe recipe, String project, boolean overrideExisting) {
        recipe.getCollections().forEach((collectionName, collection) -> {
            List<SchemaField> build = collection.build();
            List<SchemaField> fields = metastore.getOrCreateCollectionFieldList(project, collectionName,
                    ImmutableSet.copyOf(build));
            List<SchemaField> collisions = build.stream()
                    .filter(f -> fields.stream().anyMatch(field -> field.getName().equals(f.getName()) && !f.getType().equals(field.getType())))
                    .collect(Collectors.toList());

            if(!collisions.isEmpty()) {
                String errMessage = collisions.stream().map(f -> {
                    SchemaField existingField = fields.stream().filter(field -> field.getName().equals(f.getName())).findAny().get();
                    return String.format("Recipe: [%s : %s], Collection: [%s, %s]", f.getName(), f.getType(),
                            existingField.getName(), existingField.getType());
                }).collect(Collectors.joining(", "));
                String message = overrideExisting ? "Overriding collection fields is not possible." : "Collision in collection fields.";
                throw new RakamException(message + " " +errMessage, BAD_REQUEST);
            }
        });

        recipe.getContinuousQueryBuilders().stream()
                .map(builder -> builder.createContinuousQuery(project))
                .forEach(continuousQuery -> continuousQueryService.create(continuousQuery, false).getResult().whenComplete((res, ex) -> {
                    if(ex != null) {
                        if(ex instanceof AlreadyExistsException) {
                            if(overrideExisting) {
                                continuousQueryService.delete(project, continuousQuery.tableName);
                                continuousQueryService.create(continuousQuery, false);
                            } else {
                                throw Throwables.propagate(ex);
                            }
                        }
                        throw Throwables.propagate(ex);
                    }
                }));

        recipe.getMaterializedViewBuilders().stream()
                .map(builder -> builder.createMaterializedView(project))
                .forEach(materializedView -> materializedViewService.create(materializedView).whenComplete((res, ex) -> {
                    if(ex != null) {
                        if(ex instanceof AlreadyExistsException) {
                            if(overrideExisting) {
                                materializedViewService.delete(project, materializedView.tableName);
                                materializedViewService.create(materializedView);
                            } else {
                                throw Throwables.propagate(ex);
                            }
                        }
                        throw Throwables.propagate(ex);
                    }
                }));

        recipe.getReports().stream()
                .map(reportBuilder -> reportBuilder.createReport(project))
                .forEach(report -> {
                    try {
                        reportMetadata.save(null, report);
                    } catch (AlreadyExistsException e) {
                        if(overrideExisting) {
                            reportMetadata.update(null, report);
                        } else {
                            throw Throwables.propagate(e);
                        }
                    }
                });

        recipe.getCustomReports().stream()
                .map(reportBuilder -> reportBuilder.createCustomReport(project))
                .forEach(customReport -> {
                    try {
                        customReportMetadata.save(null, customReport);
                    } catch (AlreadyExistsException e) {
                        if (overrideExisting) {
                            customReportMetadata.update(customReport);
                        } else {
                            throw Throwables.propagate(e);
                        }
                    }
                });

        recipe.getCustomPages().stream()
                .map(reportBuilder -> reportBuilder.createCustomPage(project))
                .forEach(customReport -> {
                    try {
                        customPageDatabase.save(null, customReport);
                    } catch (AlreadyExistsException e) {
                        if (overrideExisting) {
                            customPageDatabase.delete(customReport.project(), customReport.slug);
                            customPageDatabase.save(null, customReport);
                        } else {
                            throw Throwables.propagate(e);
                        }
                    }
                });
    }
}
