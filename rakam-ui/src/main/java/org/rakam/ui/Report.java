package org.rakam.ui;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.plugin.ProjectItem;
import org.rakam.server.http.annotations.ApiParam;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


public class Report implements ProjectItem {
    public final String project;
    public final String slug;
    public final String name;
    public final String query;
    public final Map<String, Object> options;

    @JsonCreator
    public Report(@ApiParam(name = "project", required = true) String project,
                  @ApiParam(name = "slug", value="Short name of the report", required = true) String slug,
                  @ApiParam(name = "name", value="The name of the report", required = true) String name,
                  @ApiParam(name = "query", value="The sql query that will be executed", required = true)@JsonProperty("query") String query,
                  @ApiParam(name = "options", value="Additional information about the materialized view", required = false) Map<String, Object> options)
    {
        this.project = checkNotNull(project, "project is required");
        this.name = checkNotNull(name, "name is required");
        this.slug = checkNotNull(slug, "slug is required");
        this.query = checkNotNull(query, "query is required");
        this.options = options;

        checkArgument(this.slug.matches("^[A-Za-z]+[A-Za-z0-9_]*"),
                "slug must only contain alphanumeric characters and _");
    }

    @Override
    public String project() {
        return project;
    }
}

