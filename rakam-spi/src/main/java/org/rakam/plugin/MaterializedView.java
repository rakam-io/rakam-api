package org.rakam.plugin;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Statement;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.server.http.annotations.ApiParam;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static com.google.common.base.Preconditions.*;
import static java.time.temporal.ChronoUnit.MILLIS;


public class MaterializedView {
    private final static SqlParser SQL_PARSER = new SqlParser();

    @JsonProperty("table_name") public final String tableName;
    @JsonProperty("query") public final String query;
    @JsonProperty("incremental") public final boolean incremental;
    @JsonProperty("update_interval") public final Duration updateInterval;
    @JsonProperty("last_update") public transient Instant lastUpdate;
    @JsonProperty("name") public String name;
    @JsonProperty("options") public final Map<String, Object> options;

    @JsonCreator
    public MaterializedView(@ApiParam(value = "table_name", description="The table name of the materialized view that can be used when querying") String tableName,
                            @ApiParam(value = "name", description="Name") String name,
                            @ApiParam(value = "query", description="The sql query that will be executed and materialized") String query,
                            @ApiParam(value = "update_interval", required = false) Duration updateInterval,
                            @ApiParam(value = "incremental", required = false) Boolean incremental,
                            @ApiParam(value = "options", required = false) Map<String, Object> options) {
        this.tableName = checkNotNull(tableName, "table_name is required");
        this.name = checkNotNull(name, "name is required");
        this.query = checkNotNull(query, "query is required");
        this.incremental = incremental == null ? false : incremental;
        this.updateInterval = updateInterval;
        this.options = options;
        validateQuery();
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
        checkArgument(this.tableName.matches("^[A-Za-z]+[A-Za-z0-9_]*"),
                "table_name must only contain alphanumeric characters and _");
        checkArgument(this.tableName.matches("^[A-Za-z0-9_]*"),
                "table_name must only contain alphanumeric characters and _");
    }

    public boolean needsUpdate(Clock clock) {
        return lastUpdate == null || lastUpdate.until(clock.instant(), MILLIS) > updateInterval.toMillis();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MaterializedView that = (MaterializedView) o;

        if (incremental != that.incremental) {
            return false;
        }
        if (!tableName.equals(that.tableName)) {
            return false;
        }
        if (!query.equals(that.query)) {
            return false;
        }
        if (updateInterval != null ? !updateInterval.equals(that.updateInterval) : that.updateInterval != null) {
            return false;
        }
        return options != null ? options.equals(that.options) : that.options == null;
    }

    @Override
    public int hashCode()
    {
        int result = tableName.hashCode();
        result = 31 * result + query.hashCode();
        result = 31 * result + (incremental ? 1 : 0);
        result = 31 * result + (updateInterval != null ? updateInterval.hashCode() : 0);
        result = 31 * result + (options != null ? options.hashCode() : 0);
        return result;
    }
}
