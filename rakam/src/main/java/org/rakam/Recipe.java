package org.rakam;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.inject.Inject;
import io.airlift.units.Duration;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;
import org.rakam.ui.Report;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
* Created by buremba <Burak Emre Kabakcı> on 15/07/15 11:04.
*/
public class Recipe {
    private final Strategy strategy;
    private final String project;
    private final Map<String, Collection> collections;
    private final List<MaterializedViewBuilder> materializedViews;
    private final List<ContinuousQueryBuilder> continuousQueries;
    private final List<ReportBuilder> reports;

    @JsonCreator
    public Recipe(@JsonProperty("strategy") Strategy strategy,
                  @JsonProperty("project") String project,
                  @JsonProperty("collections") Map<String, Collection> collections,
                  @JsonProperty("materialized_queries") List<MaterializedViewBuilder> materializedQueries,
                  @JsonProperty("continuous_queries") List<ContinuousQueryBuilder> continuousQueries,
                  @JsonProperty("reports") List<ReportBuilder> reports) {
        if(strategy != Strategy.SPECIFIC && project != null) {
            throw new IllegalArgumentException("'project' parameter can be used when 'strategy' is 'specific'");
        }
        this.strategy = strategy;
        this.project = project;
        this.collections = Collections.unmodifiableMap(collections);
        this.materializedViews = materializedQueries == null ? null : Collections.unmodifiableList(materializedQueries);
        this.continuousQueries = continuousQueries == null ? null : Collections.unmodifiableList(continuousQueries);
        this.reports = Collections.unmodifiableList(reports);
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
        private final String name;
        private final String tableName;
        private final String query;
        private final List<String> collections;
        private final Map<String, Object> options;

        @JsonCreator
        public ContinuousQueryBuilder(@JsonProperty("name") String name,
                               @JsonProperty("table_name") String tableName,
                               @JsonProperty("query") String query,
                               @JsonProperty("collections") List<String> collections,
                               @JsonProperty("options") Map<String, Object> options) {
            this.name = name;
            this.tableName = tableName;
            this.query = query;
            this.collections = collections;
            this.options = options;
        }

        public ContinuousQuery createContinuousQuery(String project) {
            return new ContinuousQuery(project, name, tableName, query, collections, options);
        }
    }

    public static class ReportBuilder {
        private final String slug;
        private final String name;
        private final String query;
        private final Map<String, Object> options;

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

    public static enum Strategy {
        DEFAULT, SPECIFIC;

        @JsonCreator
        public static Strategy get(String name) {
            return valueOf(name.toUpperCase());
        }
    }
}
