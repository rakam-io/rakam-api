package org.rakam.postgresql.report;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import io.airlift.log.Logger;
import org.postgresql.util.PGobject;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.report.QueryStats;
import org.rakam.util.JsonHelper;
import org.rakam.util.SentryUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static java.lang.String.format;
import static org.rakam.postgresql.analysis.PostgresqlEventStore.UTC_CALENDAR;
import static org.rakam.postgresql.analysis.PostgresqlMetastore.fromSql;
import static org.rakam.report.QueryResult.EXECUTION_TIME;

public class PostgresqlQueryExecution implements QueryExecution {
    private final static Logger LOGGER = Logger.get(PostgresqlQueryExecution.class);

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
                    List<SchemaField> cols = ImmutableList.of(new SchemaField("result", FieldType.BOOLEAN));
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
                    error = new QueryError(cause.getMessage(), cause.getSQLState(), cause.getErrorCode(), null, null);
                    SentryUtil.logQueryError(query, error, PostgresqlQueryExecutor.class);
                } else {
                    LOGGER.error(e, "Internal query execution error");
                    error = new QueryError(e.getMessage(), null, null, null, null);
                }
                LOGGER.debug(e, format("Error while executing Postgresql query: \n%s", query));
                return QueryResult.errorResult(error);
            }
        };

        CompletableFuture<QueryResult> future = CompletableFuture.supplyAsync(task, PostgresqlQueryExecutor.QUERY_EXECUTOR);
        this.result = future;
    }

    @Override
    public QueryStats currentStats() {
        if (result.isDone()) {
            return new QueryStats(100, QueryStats.State.FINISHED, null, null, null, null, null, null);
        } else {
            return new QueryStats(0, QueryStats.State.RUNNING, null, null, null, null, null, null);
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

    private static QueryResult resultSetToQueryResult(ResultSet resultSet, long executionTimeInMillis) {
        List<SchemaField> columns;
        List<List<Object>> data;
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            columns = new ArrayList<>(columnCount);
            for (int i = 1; i < columnCount + 1; i++) {
                columns.add(new SchemaField(metaData.getColumnName(i), fromSql(metaData.getColumnType(i), metaData.getColumnTypeName(i))));
            }

            ImmutableList.Builder<List<Object>> builder = ImmutableList.builder();
            while (resultSet.next()) {
                List<Object> rowBuilder = Arrays.asList(new Object[columnCount]);
                for (int i = 0; i < columnCount; i++) {
                    Object object;
                    FieldType type = columns.get(i).getType();
                    switch (type) {
                        case STRING:
                            object = resultSet.getString(i+1);
                            break;
                        case LONG:
                            object = resultSet.getLong(i+1);
                            break;
                        case INTEGER:
                            object = resultSet.getInt(i+1);
                            break;
                        case DECIMAL:
                            object = resultSet.getBigDecimal(i+1).doubleValue();
                            break;
                        case DOUBLE:
                            object = resultSet.getDouble(i+1);
                            break;
                        case BOOLEAN:
                            object = resultSet.getBoolean(i+1);
                            break;
                        case TIMESTAMP:
                            object = resultSet.getTimestamp(i+1, UTC_CALENDAR).toInstant();
                            break;
                        case DATE:
                            object = resultSet.getDate(i+1, UTC_CALENDAR).toLocalDate();
                            break;
                        case TIME:
                            object = resultSet.getTime(i+1, UTC_CALENDAR).toLocalTime();
                            break;
                        case BINARY:
                            try {
                                object = ByteStreams.toByteArray(resultSet.getBinaryStream(i+1));
                            } catch (IOException e) {
                                LOGGER.error("Error while de-serializing BINARY type", e);
                                object = null;
                            }
                            break;
                        default:
                            if(type.isArray()) {
                                object = resultSet.getArray(i+1).getArray();
                            } else
                            if(type.isMap()) {
                                PGobject pgObject = (PGobject) resultSet.getObject(i+1);
                                if (pgObject.getType().equals("jsonb")) {
                                    object = JsonHelper.read(pgObject.getValue());
                                } else {
                                    throw new UnsupportedOperationException("Postgresql type is not supported");
                                }
                            } else {
                                throw new IllegalStateException();
                            }
                    }

                    if(resultSet.wasNull()) {
                        object = null;
                    }

                    rowBuilder.set(i, object);
                }
                builder.add(rowBuilder);
            }
            data = builder.build();
            return new QueryResult(columns, data, ImmutableMap.of(EXECUTION_TIME, executionTimeInMillis));
        } catch (SQLException e) {
            QueryError error = new QueryError(e.getMessage(), e.getSQLState(), e.getErrorCode(), null, null);
            return QueryResult.errorResult(error);
        }
    }
}
