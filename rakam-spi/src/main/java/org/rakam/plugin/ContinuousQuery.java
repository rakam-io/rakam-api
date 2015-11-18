package org.rakam.plugin;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Table;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.QueryFormatter;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;


public class ContinuousQuery implements ProjectItem {
    private final static SqlParser SQL_PARSER = new SqlParser();
    public final String project;
    public final String name;
    @JsonIgnore
    public final Query query;
    public final String tableName;
    public final List<String> collections;
    public final List<String> partitionKeys;
    public final Map<String, Object> options;

    @JsonCreator
    public ContinuousQuery(@ApiParam(name = "project", required = true) String project,
                           @ApiParam(name = "name", value="The name of the continuous query", required = true) String name,
                           @ApiParam(name = "table_name", value="The table name of the continuous query that can be used when querying", required = true) String tableName,
                           @ApiParam(name = "query", value="The sql query that will be executed and materialized", required = true) String query,
                           @ApiParam(name = "collections", value="The source collections that will be streamed", required = true) List<String> collections,
                           @ApiParam(name = "partition_keys", value="Partition columns of the table", required = false) List<String> partitionKeys,
                           @ApiParam(name = "options", value="Additional information about the continuous query", required = false) Map<String, Object> options)
            throws ParsingException, IllegalArgumentException {
        this.project = checkNotNull(project, "project is required");
        this.name = checkNotNull(name, "name is required");
        this.tableName = checkNotNull(tableName, "table_name is required");
        this.collections = collections;
        this.options = options;
        this.partitionKeys = partitionKeys == null ? ImmutableList.of() : partitionKeys;

        synchronized (this) {
            this.query = (Query) SQL_PARSER.createStatement(checkNotNull(query, "query is required"));
        }

        validateQuery();
    }

    @JsonProperty
    public String getRawQuery() {
        StringBuilder builder = new StringBuilder();
        new QueryFormatter(builder, Object::toString).process(query, 1);
        return builder.toString();
    }

    public ContinuousQuery(String project,
                           String name,
                           String tableName,
                           Query query,
                           List<String> collections,
                           List<String> partitionKeys,
                           Map<String, Object> options)
            throws ParsingException, IllegalArgumentException {
        this.project = checkNotNull(project, "project is required");
        this.name = checkNotNull(name, "name is required");
        this.tableName = checkNotNull(tableName, "table_name is required");
        this.query = checkNotNull(query, "query is required");
        this.collections = collections;
        this.options = options;
        this.partitionKeys = partitionKeys == null ? ImmutableList.of() : partitionKeys;

        validateQuery();
    }

    public void validateQuery() throws ParsingException, IllegalArgumentException {
        QueryBody queryBody = query.getQueryBody();
        // it's ugly and seems complex but actually can be expressed simply by naming variables and
        // checking the conditions and throwing the exception it they're not satisfied.
        // but since i can use the throw statement only once (otherwise, i would have to make the sting final static etc.),
        // i couldn't find a better way.
        if(!(queryBody instanceof QuerySpecification) ||
                !((QuerySpecification) queryBody).getFrom().isPresent() ||
                !(((QuerySpecification) queryBody).getFrom().get() instanceof Table) ||
                !((Table) ((QuerySpecification) queryBody).getFrom().get()).getName().getParts().equals(ImmutableList.of("stream"))) {
            throw new IllegalArgumentException("The FROM part of the query must be 'stream' for continuous queries. " +
                    "Example: 'SELECT count(1) FROM stream GROUP BY country''");
        }

        if (query.getLimit().isPresent()) {
            throw new IllegalArgumentException("Continuous queries must not have LIMIT statement");
        }
    }

    @Override
    public String project() {
        return project;
    }
}