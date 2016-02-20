package org.rakam.analysis.abtesting;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.plugin.ProjectItem;
import org.rakam.server.http.annotations.ApiParam;

import java.util.List;

public class ABTestingReport implements ProjectItem {
    public final Integer id;
    public final String project;
    public final String name;
    public final List<Variant> variants;
    public final Goal goal;
    public final Object options;
    public final String collectionName;
    public final String connectorField;

    public ABTestingReport(int id, String project, String name, List<Variant> variants, String collectionName, String connectorField, Goal goal, Object options) {
        this.id = id;
        this.project = project;
        this.name = name;
        this.variants = variants;
        this.goal = goal;
        this.collectionName = collectionName;
        this.options = options;
        this.connectorField = connectorField;
    }

    @JsonCreator
    public ABTestingReport(@ApiParam(name="project") String project,
                           @ApiParam(name="name") String name,
                           @ApiParam(name="variants") List<Variant> variants,
                           @ApiParam(name="collection_name") String collectionName,
                           @ApiParam(name="connector_field") String connectorField,
                           @ApiParam(name="goal") Goal goal,
                           @ApiParam(name="options", required = false) Object options) {
        this.options = options;
        this.id = -1;
        this.project = project;
        this.collectionName = collectionName;
        this.name = name;
        this.connectorField = connectorField;
        this.variants = variants;
        this.goal = goal;
    }

    public static class Goal {
        public final String collection;
        public final String filter;

        @JsonCreator
        public Goal(@JsonProperty("collection") String collection,
                    @JsonProperty("filter") String filter) {
            this.collection = collection;
            this.filter = filter;
        }
    }

    public static class Variant {
        public final String name;
        public final int weight;
        public final Object data;

        @JsonCreator
        public Variant(@JsonProperty("name") String name,
                       @JsonProperty("weight") int weight,
                       @JsonProperty("data") Object data) {
            this.name = name;
            this.weight = weight;
            this.data = data;
        }
    }

    @Override
    public String project() {
        return project;
    }
}
