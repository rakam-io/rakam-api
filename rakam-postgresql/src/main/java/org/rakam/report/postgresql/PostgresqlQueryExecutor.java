package org.rakam.report.postgresql;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.report.QueryStats;
import org.rakam.util.QueryFormatter;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.rakam.analysis.postgresql.PostgresqlMetastore.fromSql;

public class PostgresqlQueryExecutor implements QueryExecutor {
    final static Logger LOGGER = Logger.get(PostgresqlQueryExecutor.class);
    public final static String MATERIALIZED_VIEW_PREFIX = "_materialized_";
    public final static String CONTINUOUS_QUERY_PREFIX = "_continuous_";

    private final JDBCPoolDataSource connectionPool;
    private static final ExecutorService QUERY_EXECUTOR = new ThreadPoolExecutor(0, 50, 120L, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new ThreadFactoryBuilder()
            .setNameFormat("postgresql-query-executor")
            .setUncaughtExceptionHandler((t, e) -> e.printStackTrace()).build());
    private final Metastore metastore;
    private final QueryMetadataStore queryMetadataStore;

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


    public QueryExecution executeRawQuery(String query) {
        return new PostgresqlQueryExecution(connectionPool, query, false);
    }

    @Override
    public QueryExecution executeRawStatement(String query) {
        return new PostgresqlQueryExecution(connectionPool, query, true);
    }

    @Override
    public String formatTableReference(String project, QualifiedName name) {
        if (name.getPrefix().isPresent()) {
            switch (name.getPrefix().get().toString()) {
                case "collection":
                    return project + "." + name.getSuffix();
                case "continuous":
                    final ContinuousQuery report = queryMetadataStore.getContinuousQuery(project, name.getSuffix());
                    if(report == null) {
                        throw new RakamException(String.format("Continuous query table %s is not found", name.getSuffix()), HttpResponseStatus.BAD_REQUEST);
                    }
                    StringBuilder builder = new StringBuilder();

                    new QueryFormatter(builder, qualifiedName -> {
                        if(!qualifiedName.getPrefix().isPresent() && qualifiedName.getSuffix().equals("stream")) {
                            return replaceStream(report);
                        }
                        return project + "." + name.getSuffix();
                    }).process(report.query, 1);

                    return "("+builder.toString()+") as "+name.getSuffix();
                case "materialized":
                    return project + "." + MATERIALIZED_VIEW_PREFIX + name.getSuffix();
                default:
                    throw new IllegalArgumentException("Schema does not exist: " + name.getPrefix().get().toString());
            }
        }
        return project + "." + name.getSuffix();
    }

    public Connection getConnection() throws SQLException {
        return connectionPool.getConnection();
    }

    private String replaceStream(ContinuousQuery report) {

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

    public static class PostgresqlQueryExecution implements QueryExecution {

        private final CompletableFuture<QueryResult> result;
        private final String query;

        public PostgresqlQueryExecution(JDBCPoolDataSource connectionPool, String sqlQuery, boolean update) {
            this.query = sqlQuery;

            // TODO: unnecessary threads will be spawn
            Supplier<QueryResult> task = () -> {
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
            };

                this.result = CompletableFuture.supplyAsync(task, QUERY_EXECUTOR);

        }

        @Override
        public QueryStats currentStats() {
            if (result.isDone()) {
                return new QueryStats(100, QueryStats.State.FINISHED, null, null, null, null, null, null);
            } else {
                return new QueryStats(0, QueryStats.State.PROCESSING, null, null, null, null, null, null);
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
                        // we remove timezone
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
