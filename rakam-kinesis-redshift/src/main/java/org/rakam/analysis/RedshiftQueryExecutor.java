package org.rakam.analysis;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.rakam.aws.RedshiftMetastore;
import org.rakam.aws.RedshiftPoolDataSource;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.report.QueryStats;
import org.rakam.util.QueryFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.lang.String.format;
import static org.rakam.aws.RedshiftMetastore.fromSql;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/07/15 19:24.
 */
public class RedshiftQueryExecutor implements QueryExecutor {
    final static Logger LOGGER = LoggerFactory.getLogger(RedshiftQueryExecutor.class);
    final SqlParser parser = new SqlParser();
    public final static String CONTINUOUS_QUERY_PREFIX = "_continuous_";
    public final static String MATERIALIZED_VIEW_PREFIX = "_materialized_";

    private final RedshiftPoolDataSource connectionPool;
    private static final ExecutorService QUERY_EXECUTOR = new ThreadPoolExecutor(0, 50, 120L, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new ThreadFactoryBuilder()
            .setNameFormat("postgresql-query-executor")
            .setUncaughtExceptionHandler((t, e) -> e.printStackTrace()).build());
    private final RedshiftMetastore metastore;

    @Inject
    public RedshiftQueryExecutor(RedshiftPoolDataSource connectionPool, RedshiftMetastore metastore) {
        this.connectionPool = connectionPool;
        this.metastore = metastore;

        try (Connection connection = connectionPool.getConnection()) {
            connection.createStatement().execute("CREATE OR REPLACE FUNCTION to_unixtime(timestamp) RETURNS double precision" +
                    "    AS 'select extract(epoch from $1);'" +
                    "    LANGUAGE SQL" +
                    "    IMMUTABLE" +
                    "    RETURNS NULL ON NULL INPUT");
        } catch (SQLException e) {
            LOGGER.error("Error while creating required Postgresql procedures.", e);
        }
    }

    @Override
    public QueryExecution executeQuery(String project, String sqlQuery, int maxLimit) {
        // TODO: cache projects?
        if(!metastore.getProjects().contains(project)) {
            throw new IllegalArgumentException("project is not valid");
        }
        return executeRawQuery(buildQuery(project, sqlQuery, maxLimit));
    }

    @Override
    public QueryExecution executeQuery(String project, String sqlQuery) {
        // TODO: cache projects?
        if(!metastore.getProjects().contains(project)) {
            throw new IllegalArgumentException("project is not valid");
        }
        return executeRawQuery(buildQuery(project, sqlQuery, null));
    }

    public QueryExecution executeRawQuery(String query) {
        return new RedshiftQueryExecutor.PostgresqlQueryExecution(this, query, false);
    }

    public QueryExecution executeRawStatement(String query) {
        return new PostgresqlQueryExecution(this, query, true);
    }

    public Connection getConnection() throws SQLException {
        return connectionPool.getConnection();
    }

    private Function<QualifiedName, String> tableNameMapper(String project) {
        return node -> {
            if (node.getPrefix().isPresent()) {
                switch (node.getPrefix().get().toString()) {
                    case "continuous":
                        return project + "." + CONTINUOUS_QUERY_PREFIX + node.getSuffix();
                    case "materialized":
                        return project + "." + MATERIALIZED_VIEW_PREFIX + node.getSuffix();
                    default:
                        throw new IllegalArgumentException("Schema does not exist: " + node.getPrefix().get().toString());
                }
            }
            return project + "." + node.getSuffix();
        };
    }

    private String buildQuery(String project, String query, Integer maxLimit) {
        StringBuilder builder = new StringBuilder();
        Query statement;
        synchronized (parser) {
            statement = (Query) parser.createStatement(query);
        }

        new QueryFormatter(builder, tableNameMapper(project)).process(statement, Lists.newArrayList());

        if(maxLimit != null) {
            if (statement.getLimit().isPresent() && Long.parseLong(statement.getLimit().get()) > maxLimit) {
                throw new IllegalArgumentException(format("The maximum value of LIMIT statement is %s", statement.getLimit().get()));
            } else {
                builder.append(" LIMIT ").append(maxLimit);
            }
        }

        return builder.toString();
    }

    private String buildStatement(String project, String query) {
        StringBuilder builder = new StringBuilder();
        com.facebook.presto.sql.tree.Statement statement;
        synchronized (parser) {
            statement = parser.createStatement(query);
        }
        new QueryFormatter(builder, tableNameMapper(project)).process(statement, Lists.newArrayList());

        return builder.toString();
    }

    @Override
    public QueryExecution executeStatement(String project, String sqlQuery) {
        return executeRawStatement(buildStatement(project, sqlQuery));
    }

    public static class PostgresqlQueryExecution implements QueryExecution {

        private final CompletableFuture<QueryResult> result;
        private final String query;

        public PostgresqlQueryExecution(RedshiftQueryExecutor connectionPool, String sqlQuery, boolean update) {
            this.query = sqlQuery;

            this.result = CompletableFuture.supplyAsync(() -> {
                try (Connection connection = connectionPool.getConnection()) {
                    Statement statement = connection.createStatement();
                    if(update) {
                        statement.execute(sqlQuery);
                        // CREATE TABLE queries doesn't return any value and
                        // fail when using executeQuery so we face the result data
                        List<SchemaField> cols = ImmutableList.of(new SchemaField("result", FieldType.BOOLEAN, true));
                        List<List<Object>> data = ImmutableList.of(ImmutableList.of(true));
                        return new QueryResult(cols, data);
                    }else {
                        return resultSetToQueryResult(statement.executeQuery(sqlQuery));
                    }
                } catch (Exception e) {
                    QueryError error;
                    if(e instanceof SQLException) {
                        SQLException cause = (SQLException) e;
                        error = new QueryError(cause.getMessage(), cause.getSQLState(), cause.getErrorCode());
                    } else {
                        error = new QueryError("Internal query execution error", null, 0);
                    }
                    return QueryResult.errorResult(error);
                }
            }, QUERY_EXECUTOR);
        }

        @Override
        public QueryStats currentStats() {
            if(result.isDone()) {
                return new QueryStats(100, "FINISHED", null, null, null, null, null, null);
            } else {
                return new QueryStats(0, "PROCESSING", null, null, null, null, null, null);
            }
        }

        @Override
        public boolean isFinished() {
            return result.isDone();
        }

        @Override
        public CompletableFuture<QueryResult> getResult() {
            return result;
        }

        @Override
        public String getQuery() {
            return query;
        }
    }

    private static QueryResult resultSetToQueryResult(ResultSet resultSet) {
        List<SchemaField> columns;
        List<List<Object>> data;
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            columns = new ArrayList<>(columnCount);
            for (int i = 1; i < columnCount+1; i++) {
                columns.add(new SchemaField(metaData.getColumnName(i), fromSql(metaData.getColumnType(i)), true));
            }

            ImmutableList.Builder<List<Object>> builder = ImmutableList.builder();
            while (resultSet.next()) {
                List<Object> rowBuilder = Arrays.asList(new Object[columnCount]);
                for (int i = 1; i < columnCount+1; i++) {
                    rowBuilder.set(i - 1, resultSet.getObject(i));
                }
                builder.add(rowBuilder);
            }
            data = builder.build();
            return new QueryResult(columns, data);
        } catch (SQLException e) {
            QueryError error = new QueryError(e.getMessage(), e.getSQLState(), e.getErrorCode());
            return QueryResult.errorResult(error);
        }
    }
}