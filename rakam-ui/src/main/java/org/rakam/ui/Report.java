package org.rakam.ui;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/04/15 20:24.
 */
public class Report {
    public final String project;
    public final String slug;
    public final String name;
    public final String query;
    public final JsonNode options;

    @JsonCreator
    public Report(@JsonProperty("project") String project,
                    @JsonProperty("slug") String slug,
                    @JsonProperty("name") String name,
                    @JsonProperty("query") String query,
                    @JsonProperty("options") JsonNode options) {
        this.project = checkNotNull(project, "project is required");
        this.name = checkNotNull(name, "name is required");
        this.slug = checkNotNull(slug, "slug is required");
        this.query = checkNotNull(query, "query is required");
        this.options = options;

        checkArgument(this.slug.matches("^[A-Za-z]+[A-Za-z0-9_]*"),
                "slug must only contain alphanumeric characters and _");
    }
}

