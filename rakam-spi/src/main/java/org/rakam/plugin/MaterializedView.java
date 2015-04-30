package org.rakam.plugin;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.server.http.annotations.ApiParam;
import io.airlift.units.Duration;
import org.rakam.server.http.annotations.ApiParamIgnore;

import java.time.Instant;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 22:03.
 */
public class MaterializedView {
    private final static SqlParser SQL_PARSER = new SqlParser();
    @ApiParam(name = "project", required = true)
    public final String project;
    @ApiParam(name = "name", value="The name of the materialized view", required = true)
    public final String name;
    @ApiParam(name = "table_name", value="The table name of the materialized view that can be used when querying", required = true)
    public final String table_name;
    @ApiParam(name = "query", value="The sql query that will be executed and materialized", required = true)
    public final Statement query;
    @ApiParam(name = "options", value="Additional information about the materialized view", required = false)
    public final Map<String, Object> options;
    @ApiParam(name = "update_interval", value="", required = false)
    public final Duration updateInterval;
    @ApiParamIgnore
    public Instant lastUpdate;

    @JsonCreator
    public MaterializedView(@JsonProperty("project") String project,
                            @JsonProperty("name") String name,
                            @JsonProperty("table_name") String table_name,
                            @JsonProperty("query") String query,
                            @JsonProperty("update_interval") Duration updateInterval,
                            @JsonProperty("options") Map<String, Object> options) {
        this.project = checkNotNull(project, "project is required");
        this.name = checkNotNull(name, "name is required");
        this.table_name = checkNotNull(table_name, "table_name is required");
        synchronized (SQL_PARSER) {
            this.query = SQL_PARSER.createStatement(checkNotNull(query, "query is required"));
        }
        this.options = options;
        this.updateInterval = updateInterval;

        checkArgument(this.table_name.matches("^[A-Za-z]+[A-Za-z0-9_]*"),
                "table_name must only contain alphanumeric characters and _");
    }
}
