package org.rakam.report.abtesting;

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

    public ABTestingReport(int id, String project, String name, List<Variant> variants, Goal goal, Object options) {
        this.id = id;
        this.project = project;
        this.name = name;
        this.variants = variants;
        this.goal = goal;
        this.options = options;
    }

    @JsonCreator
    public ABTestingReport(@ApiParam("project") String project,
                           @ApiParam("name") String name,
                           @ApiParam("variants") List<Variant> variants,
                           @ApiParam("goal") Goal goal,
                           @ApiParam(value = "options", required = false) Object options) {
        this.options = options;
        this.id = -1;
        this.project = project;
        this.name = name;
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
        public final Object data;

        @JsonCreator
        public Variant(@JsonProperty("name") String name,
                       @JsonProperty("data") Object data) {
            this.name = name;
            this.data = data;
        }
    }

    @Override
    public String project() {
        return project;
    }
}
