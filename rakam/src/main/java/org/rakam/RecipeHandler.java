package org.rakam;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.NotExistsException;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.ui.JDBCReportMetadata;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RecipeHandler {
    private final Metastore metastore;
    private final ContinuousQueryService continuousQueryService;
    private final MaterializedViewService materializedViewService;
    private final JDBCReportMetadata reportMetadata;

    @Inject
    public RecipeHandler(Metastore metastore, ContinuousQueryService continuousQueryService, MaterializedViewService materializedViewService, JDBCReportMetadata reportMetadata) {
        this.metastore = metastore;
        this.materializedViewService = materializedViewService;
        this.continuousQueryService = continuousQueryService;
        this.reportMetadata = reportMetadata;
    }

    public Recipe export(String project) {
        final Map<String, Recipe.Collection> collections = metastore.getCollections(project).entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> {
            List<Map<String, Recipe.SchemaFieldInfo>> map = e.getValue().stream()
                    .map(a -> ImmutableMap.of(a.getName(), new Recipe.SchemaFieldInfo(a.getCategory(), a.getType(), a.isNullable())))
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
                .getReports(project).stream()
                .map(r -> new Recipe.ReportBuilder(r.slug, r.name, r.query, r.options))
                .collect(Collectors.toList());

        return new Recipe(Recipe.Strategy.SPECIFIC, project, collections, materializedViews, continuousQueryBuilders, reports);
    }

    public void install(Recipe recipe, String project) {
        installInternal(recipe, project);
    }

    public void install(Recipe recipe) {
        if(recipe.getProject() != null) {
            installInternal(recipe, recipe.getProject());
        } else {
            throw new IllegalArgumentException("project is null");
        }
    }

    public void installInternal(Recipe recipe, String project) {

        recipe.getCollections().forEach((collectionName, schema) -> {
            try {
                metastore.getOrCreateCollectionFieldList(project, collectionName, new HashSet<>(schema.getColumns()));
            } catch (NotExistsException e) {
                throw Throwables.propagate(e);
            }
        });

        recipe.getContinuousQueryBuilders().stream()
                .map(builder -> builder.createContinuousQuery(project))
                .forEach(continuousQuery ->
                        continuousQueryService.create(continuousQuery));

        recipe.getMaterializedViewBuilders().stream()
                .map(builder -> builder.createMaterializedView(project))
                .forEach(continuousQuery ->
                        materializedViewService.create(continuousQuery));

        recipe.getReports().stream()
                .map(reportBuilder -> reportBuilder.createReport(project))
                .forEach(report -> reportMetadata.save(report));
    }
}
