package org.rakam.analysis.abtesting;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.server.http.annotations.ApiParam;

import java.util.List;

public class ABTestingReport {
    public final Integer id;
    public final String name;
    public final List<Variant> variants;
    public final Goal goal;
    public final Object options;
    public final String collectionName;
    public final String connectorField;

    public ABTestingReport(int id, String name, List<Variant> variants, String collectionName, String connectorField, Goal goal, Object options) {
        this.id = id;
        this.name = name;
        this.variants = variants;
        this.goal = goal;
        this.collectionName = collectionName;
        this.options = options;
        this.connectorField = connectorField;
    }

    @JsonCreator
    public ABTestingReport(@ApiParam("name") String name,
                           @ApiParam("variants") List<Variant> variants,
                           @ApiParam("collection_name") String collectionName,
                           @ApiParam("connector_field") String connectorField,
                           @ApiParam("goal") Goal goal,
                           @ApiParam(value = "options", required = false) Object options) {
        this.options = options;
        this.id = -1;
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
        public Goal(@ApiParam("collection") String collection,
                    @ApiParam(value = "filter", required = false) String filter) {
            this.collection = collection;
            this.filter = filter;
        }
    }

    public static class Variant {
        public final String name;
        public final int weight;
        public final Object data;

        @JsonCreator
        public Variant(@ApiParam("name") String name,
                       @ApiParam("weight") int weight,
                       @ApiParam("data") Object data) {
            this.name = name;
            this.weight = weight;
            this.data = data;
        }
    }
}
