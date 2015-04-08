package org.rakam.plugin;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 22:03.
 */
public class Report {
    // not multithreaded :(
    private final static SqlParser SQL_PARSER = new SqlParser();

    public final String project;
    public final String name;
    public final String tableName;
    public final Statement query;
    public final JsonNode options;

    @JsonCreator
    public Report(@JsonProperty("project") String project,
                  @JsonProperty("name") String name,
                  @JsonProperty("table_name") String tableName,
                  @JsonProperty("query") String query,
                  @JsonProperty("options")  JsonNode options) {
        this.project = checkNotNull(project, "project is required");
        this.name = checkNotNull(name, "name is required");
        this.tableName = checkNotNull(tableName, "table_name is required");
        this.query = SQL_PARSER.createStatement(checkNotNull(query, "query is required"));
        this.options = options;

        checkArgument(this.tableName.matches("^[A-Za-z]+[A-Za-z0-9_]*"),
                "table_name must only contain alphanumeric characters and _");
    }
}
