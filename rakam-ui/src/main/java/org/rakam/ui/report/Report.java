package org.rakam.ui.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.server.http.annotations.ApiParam;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


public class Report {
    public final String slug;
    public final String category;
    public final String name;
    public final String query;
    public final boolean shared;
    public final Map<String, Object> options;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Boolean hasPermission;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer userId;

    @JsonCreator
    public Report(@ApiParam(value="slug", description="Short name of the report") String slug,
                  @ApiParam(value="category", description="Category of the report", required = false) String category,
                  @ApiParam(value="name", description="The name of the report") String name,
                  @ApiParam(value="query", description="The sql query that will be executed") @JsonProperty("query") String query,
                  @ApiParam(value="options", description="Additional information about the materialized view", required = false) Map<String, Object> options,
                  @ApiParam(value="shared", required = false, description="Shared with other users") boolean shared)
    {
        this.name = checkNotNull(name, "name is required");
        this.slug = checkNotNull(slug, "slug is required");
        this.query = checkNotNull(query, "query is required");
        this.options = options;
        this.shared = shared;
        this.category = category;

        checkArgument(this.slug.matches("^[A-Za-z]+[A-Za-z0-9_]*"),
                "slug must only contain alphanumeric characters and _");
    }

    public void setPermission(boolean hasPermission) {
        this.hasPermission = hasPermission;
    }

    public void setUserId(int user) {
        this.userId = user;
    }
}

