package org.rakam.plugin;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Statement;
import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.server.http.annotations.ApiParam;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;


public class MaterializedView implements ProjectItem {
    private final static SqlParser SQL_PARSER = new SqlParser();

    public final String project;
    public final String name;
    public final String table_name;
    public final String query;
    public final Map<String, Object> options;
    public final Duration updateInterval;
    public Instant lastUpdate;

    @JsonCreator
    public MaterializedView(@ApiParam(name = "project", required = true) String project,
                            @ApiParam(name = "name", value="The name of the materialized view", required = true) String name,
                            @ApiParam(name = "table_name", value="The table name of the materialized view that can be used when querying", required = true) String table_name,
                            @ApiParam(name = "query", value="The sql query that will be executed and materialized", required = true) String query,
                            @ApiParam(name = "update_interval", value="", required = false) Duration updateInterval,
                            @ApiParam(name = "options", value="Additional information about the materialized view", required = false) Map<String, Object> options) {
        this.project = checkNotNull(project, "project is required");
        this.name = checkNotNull(name, "name is required");
        this.table_name = checkNotNull(table_name, "table_name is required");
        this.query = checkNotNull(query, "query is required");
        this.options = options;
        this.updateInterval = updateInterval;
    }

    public void validateQuery() {
        Statement query;
        synchronized (SQL_PARSER) {
            query = SQL_PARSER.createStatement(this.query);
        }
        checkState(query instanceof Query, "Expression is not query");
        checkState((!((Query) query).getLimit().isPresent()),
                "The query of materialized view can't contain LIMIT statement");
        checkState(!(((QuerySpecification) ((Query) query).getQueryBody()).getLimit().isPresent()),
                "The query of materialized view can't contain LIMIT statement");
        checkArgument(this.table_name.matches("^[A-Za-z]+[A-Za-z0-9_]*"),
                "table_name must only contain alphanumeric characters and _");
    }

    @Override
    public String project() {
        return project;
    }
}
