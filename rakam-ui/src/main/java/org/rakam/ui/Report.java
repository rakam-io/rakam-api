package org.rakam.ui;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.server.http.annotations.ApiParam;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/04/15 20:24.
 */
public class Report {
    @ApiParam(name = "project", required = true)
    public final String project;
    @ApiParam(name = "name", value="Short name of the report", required = true)
    public final String slug;
    @ApiParam(name = "name", value="The name of the report", required = true)
    public final String name;
    @ApiParam(name = "query", value="The sql query that will be executed", required = true)
    public final String query;
    @ApiParam(name = "options", value="Additional information about the materialized view", required = false)
    public final Map<String, Object> options;

    @JsonCreator
    public Report(@JsonProperty("project") String project,
                    @JsonProperty("slug") String slug,
                    @JsonProperty("name") String name,
                    @JsonProperty("query") String query,
                    @JsonProperty("options") Map<String, Object> options) {
        this.project = checkNotNull(project, "project is required");
        this.name = checkNotNull(name, "name is required");
        this.slug = checkNotNull(slug, "slug is required");
        this.query = checkNotNull(query, "query is required");
        this.options = options;

        checkArgument(this.slug.matches("^[A-Za-z]+[A-Za-z0-9_]*"),
                "slug must only contain alphanumeric characters and _");
    }
}

