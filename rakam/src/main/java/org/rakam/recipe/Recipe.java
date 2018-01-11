package org.rakam.recipe;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.MaterializedView;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Recipe {
    private final Strategy strategy;
    private final Map<String, CollectionDefinition> collections;
    private final List<MaterializedView> materializedViews;

    @JsonCreator
    public Recipe(@JsonProperty("strategy") Strategy strategy,
                  @JsonProperty("collections") Map<String, CollectionDefinition> collections,
                  @JsonProperty("materialized_views") List<MaterializedView> materializedQueries) {
        this.strategy = strategy;
        this.collections = collections != null ? ImmutableMap.copyOf(collections) : ImmutableMap.of();
        this.materializedViews = materializedQueries == null ? ImmutableList.of() : ImmutableList.copyOf(materializedQueries);
    }

    @JsonProperty("strategy")
    public Strategy getStrategy() {
        return strategy;
    }

    @JsonProperty("collections")
    public Map<String, CollectionDefinition> getCollections() {
        return collections;
    }

    @JsonProperty("materialized_views")
    public List<MaterializedView> getMaterializedViewBuilders() {
        return materializedViews;
    }

    public enum Strategy {
        DEFAULT, SPECIFIC;

        @JsonCreator
        public static Strategy get(String name) {
            return valueOf(name.toUpperCase());
        }
    }

    public static class CollectionDefinition {
        public final List<Map<String, SchemaFieldInfo>> columns;

        @JsonCreator
        public CollectionDefinition(@JsonProperty("columns") List<Map<String, SchemaFieldInfo>> columns) {
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
}
