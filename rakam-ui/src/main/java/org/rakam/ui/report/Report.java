package org.rakam.ui.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

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
    public final Map<String, Object> queryOptions;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer userId;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String userEmail;

    @JsonCreator
    public Report(@JsonProperty(value = "slug", required = true) String slug,
                  @JsonProperty(value = "category") String category,
                  @JsonProperty(value = "name") String name,
                  @JsonProperty(value = "query") String query,
                  @JsonProperty(value = "options") Map<String, Object> options,
                  @JsonProperty(value = "queryOptions") Map<String, Object> queryOptions,
                  @JsonProperty(value = "shared") boolean shared) {
        this.name = checkNotNull(name, "name is required");
        this.slug = checkNotNull(slug, "slug is required");
        this.query = checkNotNull(query, "query is required");
        this.options = options;
        this.shared = shared;
        this.queryOptions = queryOptions == null ? ImmutableMap.of() : queryOptions;
        this.category = category;

        checkArgument(this.slug.matches("^[A-Za-z]+[A-Za-z0-9_]*"),
                "slug must only contain alphanumeric characters and _");
    }

    public void setUserId(int user) {
        this.userId = user;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }
}

