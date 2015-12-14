package org.rakam;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;
import org.rakam.plugin.ProjectItem;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.ui.Report;

import javax.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Recipe implements ProjectItem {
    public final Strategy strategy;
    public final String project;
    public final Map<String, Collection> collections;
    public final List<MaterializedViewBuilder> materializedViews;
    public final List<ContinuousQueryBuilder> continuousQueries;
    public final List<ReportBuilder> reports;

    @JsonCreator
    public Recipe(@ApiParam(name="strategy") Strategy strategy,
                  @ApiParam(name="project", required = false) String project,
                  @ApiParam(name="collections") Map<String, Collection> collections,
                  @ApiParam(name="materialized_queries") List<MaterializedViewBuilder> materializedQueries,
                  @ApiParam(name="continuous_queries") List<ContinuousQueryBuilder> continuousQueries,
                  @ApiParam(name="reports") List<ReportBuilder> reports) {
        if(strategy != Strategy.SPECIFIC && project != null) {
            throw new IllegalArgumentException("'project' parameter can be used when 'strategy' is 'specific'");
        }
        this.strategy = strategy;
        this.project = project;
        this.collections = ImmutableMap.copyOf(collections);
        this.materializedViews = materializedQueries == null ? null : ImmutableList.copyOf(materializedQueries);
        this.continuousQueries = continuousQueries == null ? null : ImmutableList.copyOf(continuousQueries);
        this.reports = ImmutableList.copyOf(reports);
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public String getProject() {
        return project;
    }

    public Map<String, Collection> getCollections() {
        return collections;
    }

    public List<MaterializedViewBuilder> getMaterializedViewBuilders() {
        return materializedViews;
    }

    public List<ContinuousQueryBuilder> getContinuousQueryBuilders() {
        return continuousQueries;
    }

    public List<ReportBuilder> getReports() {
        return reports;
    }

    @Override
    public String project() {
        return project;
    }

    public static class Collection {
        private final List<SchemaField> columns;

        @JsonCreator
        public Collection(@JsonProperty("columns") List<Map<String, SchemaFieldInfo>> columns) {
            this.columns = columns.stream()
                    .map(column -> {
                        Map.Entry<String, SchemaFieldInfo> next = column.entrySet().iterator().next();
                        return new SchemaField(next.getKey(), next.getValue().type, next.getValue().isNullable());
                    }).collect(Collectors.toList());
        }

        public List<SchemaField> getColumns() {
            return columns;
        }
    }

    public static class MaterializedViewBuilder {
        public final String name;
        public final String table_name;
        public final String query;
        public final Map<String, Object> options;
        public final Duration updateInterval;

        @Inject
        public MaterializedViewBuilder(@JsonProperty("name") String name, @JsonProperty("table_name") String table_name, @JsonProperty("query") String query, @JsonProperty("update_interval") Duration updateInterval, @JsonProperty("options") Map<String, Object> options) {
            this.name = name;
            this.table_name = table_name;
            this.query = query;
            this.options = options;
            this.updateInterval = updateInterval;
        }

        public MaterializedView createMaterializedView(String project) {
            return new MaterializedView(project, name, table_name, query, updateInterval, options);
        }
    }

    public static class ContinuousQueryBuilder {
        public final String name;
        public final String tableName;
        public final String query;
        public final List<String> partitionKeys;
        public final Map<String, Object> options;

        @JsonCreator
        public ContinuousQueryBuilder(@JsonProperty("name") String name,
                                      @JsonProperty("table_name") String tableName,
                                      @JsonProperty("query") String query,
                                      @JsonProperty("partition_keys") List<String> partitionKeys,
                                      @JsonProperty("options") Map<String, Object> options) {
            this.name = name;
            this.tableName = tableName;
            this.query = query;
            this.partitionKeys = partitionKeys;
            this.options = options;
        }

        public ContinuousQuery createContinuousQuery(String project) {
            return new ContinuousQuery(project, name, tableName, query, partitionKeys, options);
        }
    }

    public static class ReportBuilder {
        public final String slug;
        public final String name;
        public final String query;
        public final Map<String, Object> options;

        @JsonCreator
        public ReportBuilder(@JsonProperty("slug") String slug,
                             @JsonProperty("name") String name,
                             @JsonProperty("query") String query,
                             @JsonProperty("options") Map<String, Object> options) {
            this.slug = slug;
            this.name = name;
            this.query = query;
            this.options = options;
        }

        public Report createReport(String project) {
            return new Report(project, slug, name, query, options);
        }
    }

    public static class SchemaFieldInfo {
        private final String category;
        private final FieldType type;
        private final boolean nullable;

        @JsonCreator
        public SchemaFieldInfo(@JsonProperty("category") String category,
                               @JsonProperty("type") FieldType type,
                               @JsonProperty("nullable") Boolean nullable) {
            this.category = category;
            this.type = type;
            this.nullable = nullable == null ? true : nullable;
        }

        public FieldType getType() {
            return type;
        }

        public boolean isNullable() {
            return nullable;
        }

        public String getCategory() {
            return category;
        }
    }

    public enum Strategy {
        DEFAULT, SPECIFIC;

        @JsonCreator
        public static Strategy get(String name) {
            return valueOf(name.toUpperCase());
        }
    }
}
