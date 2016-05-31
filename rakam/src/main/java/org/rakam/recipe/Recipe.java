package org.rakam.recipe;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;

import javax.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Recipe {
    private final Strategy strategy;
    private final String project;
    private final Map<String, Collection> collections;
    private final List<MaterializedViewBuilder> materializedViews;
    private final List<ContinuousQueryBuilder> continuousQueries;

    @JsonCreator
    public Recipe(@JsonProperty("strategy") Strategy strategy,
                  @JsonProperty("project") String project,
                  @JsonProperty("collections") Map<String, Collection> collections,
                  @JsonProperty("materialized_views") List<MaterializedViewBuilder> materializedQueries,
                  @JsonProperty("continuous_queries") List<ContinuousQueryBuilder> continuousQueries) {
        if (strategy != Strategy.SPECIFIC && project != null) {
            throw new IllegalArgumentException("'project' parameter can be used when 'strategy' is 'specific'");
        }
        this.strategy = strategy;
        this.project = project;
        this.collections = collections != null ? ImmutableMap.copyOf(collections) : ImmutableMap.of();
        this.materializedViews = materializedQueries == null ? ImmutableList.of() : ImmutableList.copyOf(materializedQueries);
        this.continuousQueries = continuousQueries == null ? ImmutableList.of() : ImmutableList.copyOf(continuousQueries);
    }

    @JsonProperty("strategy")
    public Strategy getStrategy() {
        return strategy;
    }

    @JsonProperty("project")
    public String getProject() {
        return project;
    }

    @JsonProperty("collections")
    public Map<String, Collection> getCollections() {
        return collections;
    }

    @JsonProperty("materialized_views")
    public List<MaterializedViewBuilder> getMaterializedViewBuilders() {
        return materializedViews;
    }

    @JsonProperty("continuous_queries")
    public List<ContinuousQueryBuilder> getContinuousQueryBuilders() {
        return continuousQueries;
    }

    public static class Collection {
        public final List<Map<String, SchemaFieldInfo>> columns;

        @JsonCreator
        public Collection(@JsonProperty("columns") List<Map<String, SchemaFieldInfo>> columns) {
            this.columns = columns;
        }

        @JsonIgnore
        public List<SchemaField> build() {
            return columns.stream()
                    .map(column -> {
                        Map.Entry<String, SchemaFieldInfo> next = column.entrySet().iterator().next();
                        return new SchemaField(next.getKey(), next.getValue().type);
                    }).collect(Collectors.toList());
        }
    }

    public static class MaterializedViewBuilder {
        public final String name;
        public final String table_name;
        public final String query;
        public final boolean incremental;
        public final Duration updateInterval;

        @Inject
        public MaterializedViewBuilder(@JsonProperty("name") String name, @JsonProperty("table_name") String table_name, @JsonProperty("query") String query, @JsonProperty("update_interval") Duration updateInterval, @JsonProperty("incremental") Boolean incremental) {
            this.name = name;
            this.table_name = table_name;
            this.query = query;
            this.incremental = incremental;
            this.updateInterval = updateInterval;
        }

        public MaterializedView createMaterializedView(String project) {
            return new MaterializedView(name, table_name, query, updateInterval, incremental, ImmutableMap.of());
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
            return new ContinuousQuery(name, tableName, query, partitionKeys, options);
        }
    }

    public static class SchemaFieldInfo {
        public final String category;
        public final FieldType type;

        @JsonCreator
        public SchemaFieldInfo(@JsonProperty("category") String category,
                               @JsonProperty("type") FieldType type) {
            this.category = category;
            this.type = type;
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
