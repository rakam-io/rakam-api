package org.rakam.report.postgresql;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.report.QueryStats;
import org.rakam.util.QueryFormatter;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.rakam.analysis.postgresql.PostgresqlMetastore.fromSql;

public class PostgresqlQueryExecutor implements QueryExecutor {
    final static Logger LOGGER = Logger.get(PostgresqlQueryExecutor.class);
    final SqlParser parser = new SqlParser();
    public final static String CONTINUOUS_QUERY_PREFIX = "_continuous_";
    public final static String MATERIALIZED_VIEW_PREFIX = "_materialized_";

    private final JDBCPoolDataSource connectionPool;
    private final QueryMetadataStore queryMetadataStore;
    private static final ExecutorService QUERY_EXECUTOR = new ThreadPoolExecutor(0, 50, 120L, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new ThreadFactoryBuilder()
            .setNameFormat("postgresql-query-executor")
            .setUncaughtExceptionHandler((t, e) -> e.printStackTrace()).build());
    private final Metastore metastore;
    private volatile Set<String> projectCache;

    @Inject
    public PostgresqlQueryExecutor(@Named("store.adapter.postgresql") JDBCPoolDataSource connectionPool, QueryMetadataStore queryMetadataStore, Metastore metastore) {
        this.connectionPool = connectionPool;
        this.queryMetadataStore = queryMetadataStore;
        this.metastore = metastore;

        try (Connection connection = connectionPool.getConnection()) {
            connection.createStatement().execute("CREATE OR REPLACE FUNCTION to_unixtime(timestamp) RETURNS double precision" +
                    "    AS 'select extract(epoch from $1);'" +
                    "    LANGUAGE SQL" +
                    "    IMMUTABLE" +
                    "    RETURNS NULL ON NULL INPUT");
        } catch (SQLException e) {
            LOGGER.error(e, "Error while creating required Postgresql procedures.");
        }
    }

    private synchronized void updateProjectCache() {
        projectCache = metastore.getProjects();
    }

    @Override
    public QueryExecution executeQuery(String project, String sqlQuery, int maxLimit) {
        if (!projectExists(project)) {
            throw new IllegalArgumentException("Project is not valid");
        }
        return executeRawQuery(buildQuery(project, sqlQuery, maxLimit));
    }

    private boolean projectExists(String project) {
        if (projectCache == null) {
            updateProjectCache();
        }

        if (!projectCache.contains(project)) {
            updateProjectCache();
            if (!projectCache.contains(project)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public QueryExecution executeQuery(String project, String sqlQuery) {
        if (!projectExists(project)) {
            throw new IllegalArgumentException("Project is not valid");
        }
        return executeRawQuery(buildQuery(project, sqlQuery, null));
    }

    public QueryExecution executeRawQuery(String query) {
        return new PostgresqlQueryExecution(connectionPool, query, false);
    }

    public QueryExecution executeRawStatement(String query) {
        return new PostgresqlQueryExecution(connectionPool, query, true);
    }

    public Connection getConnection() throws SQLException {
        return connectionPool.getConnection();
    }

    private String replaceStream(ContinuousQuery report) {

//                        if(!node.getPrefix().isPresent() && node.getSuffix().equals("stream")) {
        if (report.collections != null && report.collections.size() == 1) {
            return report.project + "." + report.collections.get(0);
        }

        final List<Map.Entry<String, List<SchemaField>>> collect = metastore.getCollections(report.project)
                .entrySet().stream()
                .filter(c -> report.collections == null || report.collections.contains(c.getKey()))
                .collect(Collectors.toList());

        Iterator<Map.Entry<String, List<SchemaField>>> entries = collect.iterator();

        List<SchemaField> base = null;
        while (entries.hasNext()) {
            Map.Entry<String, List<SchemaField>> next = entries.next();

            if (base == null) {
                base = new ArrayList(next.getValue());
                continue;
            }

            Iterator<SchemaField> iterator = base.iterator();
            while (iterator.hasNext()) {
                if (!next.getValue().contains(iterator.next())) {
                    iterator.remove();
                }
            }
        }

        String commonColumns = (base == null ? ImmutableList.<SchemaField>of() : base).stream().map(SchemaField::getName).collect(Collectors.joining(", "));

        return "(" + collect.stream().map(c -> String.format("select %s from %s.%s", commonColumns, report.project, c.getKey()))
                .collect(Collectors.joining(" union all ")) + ") as stream";
    }

    Function<QualifiedName, String> tableNameMapper(String project) {
        return node -> {
            if (node.getPrefix().isPresent()) {
                switch (node.getPrefix().get().toString()) {
                    case "collection":
                        return project + "." + node.getSuffix();
                    case "continuous":
                        final ContinuousQuery report = queryMetadataStore.getContinuousQuery(project, node.getSuffix());
                        StringBuilder builder = new StringBuilder();
                        Query statement;
                        synchronized (parser) {
                            statement = (Query) parser.createStatement(report.query);
                        }

                        new QueryFormatter(builder, qualifiedName -> {
                            if(!qualifiedName.getPrefix().isPresent() && qualifiedName.getSuffix().equals("stream")) {
                                return replaceStream(report);
                            }
                            throw new IllegalStateException();
                        }).process(statement, 1);

                        return "("+builder.toString()+") as "+node.getSuffix();
                    case "materialized":
                        MaterializedView materializedView = queryMetadataStore.getMaterializedView(project, node.getSuffix());
                        Duration updateInterval = materializedView.updateInterval;
                        if (updateInterval == null || materializedView.lastUpdate.until(Instant.now(), ChronoUnit.MILLIS) > updateInterval.toMillis()) {
//                            queryMetadataStore.updateMaterializedView(materializedView.project, materializedView.table_name, );
                        }
                        return project + "." + MATERIALIZED_VIEW_PREFIX + node.getSuffix();
                    default:
                        throw new IllegalArgumentException("Schema does not exist: " + node.getPrefix().get().toString());
                }
            }
            return project + "." + node.getSuffix();
        };
    }

    public String buildQuery(String project, String query, Integer maxLimit) {
        StringBuilder builder = new StringBuilder();
        Query statement;
        synchronized (parser) {
            statement = (Query) parser.createStatement(query);
        }

        new QueryFormatter(builder, tableNameMapper(project)).process(statement, 1);

        if (maxLimit != null) {
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
        new QueryFormatter(builder, tableNameMapper(project)).process(statement, 1);

        return builder.toString();
    }

    @Override
    public QueryExecution executeStatement(String project, String sqlQuery) {
        return executeRawStatement(buildStatement(project, sqlQuery));
    }

    public static class PostgresqlQueryExecution implements QueryExecution {

        private final CompletableFuture<QueryResult> result;
        private final String query;

        public PostgresqlQueryExecution(JDBCPoolDataSource connectionPool, String sqlQuery, boolean update) {
            this.query = sqlQuery;

            // TODO: unnecessary threads will be spawn
            this.result = CompletableFuture.supplyAsync(() -> {
                try (Connection connection = connectionPool.getConnection()) {
                    Statement statement = connection.createStatement();
                    if (update) {
                        statement.executeUpdate(sqlQuery);
                        // CREATE TABLE queries doesn't return any value and
                        // fail when using executeQuery so we face the result data
                        List<SchemaField> cols = ImmutableList.of(new SchemaField("result", FieldType.BOOLEAN, true));
                        List<List<Object>> data = ImmutableList.of(ImmutableList.of(true));
                        return new QueryResult(cols, data);
                    } else {
                        long beforeExecuted = System.currentTimeMillis();
                        ResultSet resultSet = statement.executeQuery(sqlQuery);
                        final QueryResult queryResult = resultSetToQueryResult(resultSet,
                                System.currentTimeMillis() - beforeExecuted);
                        return queryResult;
                    }
                } catch (Exception e) {
                    QueryError error;
                    if (e instanceof SQLException) {
                        SQLException cause = (SQLException) e;
                        error = new QueryError(cause.getMessage(), cause.getSQLState(), cause.getErrorCode(), query);
                    } else {
                        error = new QueryError("Internal query execution error", null, 0, query);
                    }
                    LOGGER.debug(e, format("Error while executing Postgresql query: \n%s", query));
                    return QueryResult.errorResult(error);
                }
            }, QUERY_EXECUTOR);
        }

        @Override
        public QueryStats currentStats() {
            if (result.isDone()) {
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

        @Override
        public void kill() {
            // TODO: Find a way to kill Postgresql query.
        }
    }

    private static QueryResult resultSetToQueryResult(ResultSet resultSet, long executionTimeInMillis) {
        List<SchemaField> columns;
        List<List<Object>> data;
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            columns = new ArrayList<>(columnCount);
            for (int i = 1; i < columnCount + 1; i++) {
                columns.add(new SchemaField(metaData.getColumnName(i), fromSql(metaData.getColumnType(i)), true));
            }

            ImmutableList.Builder<List<Object>> builder = ImmutableList.builder();
            while (resultSet.next()) {
                List<Object> rowBuilder = Arrays.asList(new Object[columnCount]);
                for (int i = 1; i < columnCount + 1; i++) {
                    Object object = resultSet.getObject(i);
                    if (object instanceof Timestamp) {
                        // we have to remove zone from java.sql.Timestamp but I couldn't figure out how to do this in JDBC level.
                        object = ((Timestamp) object).toInstant();
                    }
                    rowBuilder.set(i - 1, object);
                }
                builder.add(rowBuilder);
            }
            data = builder.build();
            return new QueryResult(columns, data, ImmutableMap.of(QueryResult.EXECUTION_TIME, executionTimeInMillis));
        } catch (SQLException e) {
            QueryError error = new QueryError(e.getMessage(), e.getSQLState(), e.getErrorCode());
            return QueryResult.errorResult(error);
        }
    }
}
