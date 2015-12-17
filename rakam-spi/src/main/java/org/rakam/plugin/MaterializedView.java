package org.rakam.plugin;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Statement;
import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.server.http.annotations.ApiParam;

import java.time.Duration;
import java.time.Instant;

import static com.google.common.base.Preconditions.*;
import static org.rakam.util.ValidationUtil.checkTableColumn;


public class MaterializedView implements ProjectItem {
    private final static SqlParser SQL_PARSER = new SqlParser();

    public final String project;
    public final String name;
    public final String tableName;
    public final String query;
    public final Duration updateInterval;
    public final String incrementalField;
    public Instant lastUpdate;

    @JsonCreator
    public MaterializedView(@ApiParam(name = "project", required = true) String project,
                            @ApiParam(name = "name", value="The name of the materialized view", required = true) String name,
                            @ApiParam(name = "table_name", value="The table name of the materialized view that can be used when querying", required = true) String tableName,
                            @ApiParam(name = "query", value="The sql query that will be executed and materialized", required = true) String query,
                            @ApiParam(name = "update_interval", value="", required = false) Duration updateInterval,
                            @ApiParam(name = "incremental_field", value="", required = false) String incrementalField) {
        this.project = checkNotNull(project, "project is required");
        this.name = checkNotNull(name, "name is required");
        this.tableName = checkNotNull(tableName, "table_name is required");
        this.query = checkNotNull(query, "query is required");
        this.incrementalField = incrementalField;
        this.updateInterval = updateInterval;
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
        if(this.incrementalField != null)
            checkTableColumn(this.incrementalField, "incremental field is invalid");
    }

    @Override
    public String project() {
        return project;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MaterializedView)) return false;

        MaterializedView that = (MaterializedView) o;

        if (!project.equals(that.project)) return false;
        if (!name.equals(that.name)) return false;
        if (!tableName.equals(that.tableName)) return false;
        if (!query.equals(that.query)) return false;
        if (updateInterval != null ? !updateInterval.equals(that.updateInterval) : that.updateInterval != null)
            return false;
        return !(incrementalField != null ? !incrementalField.equals(that.incrementalField) : that.incrementalField != null);

    }

    @Override
    public int hashCode() {
        int result = project.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + tableName.hashCode();
        result = 31 * result + query.hashCode();
        result = 31 * result + (updateInterval != null ? updateInterval.hashCode() : 0);
        result = 31 * result + (incrementalField != null ? incrementalField.hashCode() : 0);
        return result;
    }
}
