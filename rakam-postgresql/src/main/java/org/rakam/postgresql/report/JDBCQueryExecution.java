package org.rakam.postgresql.report;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import io.airlift.log.Logger;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.PGobject;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.report.QueryStats;
import org.rakam.util.JsonHelper;
import org.rakam.util.LogUtil;
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static java.time.format.TextStyle.NARROW;
import static java.util.Locale.ENGLISH;
import static org.rakam.collection.FieldType.STRING;
import static org.rakam.postgresql.report.PostgresqlQueryExecutor.QUERY_EXECUTOR;
import static org.rakam.report.QueryResult.EXECUTION_TIME;
import static org.rakam.report.QueryResult.QUERY;
import static org.rakam.report.QueryStats.State.FINISHED;
import static org.rakam.report.QueryStats.State.RUNNING;
import static org.rakam.util.JDBCUtil.fromSql;
import static org.rakam.util.ValidationUtil.checkLiteral;

public class JDBCQueryExecution
        implements QueryExecution {
    private final static Logger LOGGER = Logger.get(JDBCQueryExecution.class);
    private static final ZoneId UTC = ZoneId.of("UTC");

    private final CompletableFuture<QueryResult> result;
    private final String query;
    private final ZoneId zoneId;
    private final boolean update;
    private Statement statement;

    public JDBCQueryExecution(ConnectionFactory connectionPool, String query, boolean update, Optional<ZoneId> optionalZoneId, boolean applyZone) {
        this.query = query;
        zoneId = applyZone ? optionalZoneId.map(v -> v == ZoneOffset.UTC ? UTC : v).orElse(UTC) : null;
        this.update = update;

        this.result = CompletableFuture.supplyAsync(() -> {
            Instant now = Instant.now();
            now.toString();

            final QueryResult queryResult;
            try (Connection connection = connectionPool.openConnection()) {
                statement = connection.createStatement();
                if (update) {
                    statement.executeUpdate(query);
                    // CREATE TABLE queries doesn't return any value and
                    // fail when using executeQuery so we fake the result data
                    queryResult = new QueryResult(ImmutableList.of(new SchemaField("result", FieldType.BOOLEAN)),
                            ImmutableList.of(ImmutableList.of(true)));
                } else {
                    long beforeExecuted = System.currentTimeMillis();
                    String finalQuery;
                    if (applyZone) {
                        finalQuery = format("set time zone '%s'",
                                checkLiteral(zoneId.getDisplayName(NARROW, ENGLISH))) + "; " + query;
                    } else {
                        finalQuery = query;
                    }

                    statement.execute(finalQuery);
                    if (applyZone) {
                        statement.getMoreResults();
                    }
                    ResultSet resultSet = statement.getResultSet();
                    statement = null;
                    queryResult = resultSetToQueryResult(resultSet, System.currentTimeMillis() - beforeExecuted, connection);
                }
            } catch (Exception e) {
                QueryError error;
                if (e instanceof SQLException) {
                    SQLException cause = (SQLException) e;
                    error = new QueryError(cause.getMessage(), cause.getSQLState(), cause.getErrorCode(), null, null);
                    LogUtil.logQueryError(query, error, PostgresqlQueryExecutor.class);
                } else {
                    LOGGER.error(e, "Internal query execution error");
                    error = new QueryError(e.getMessage(), null, null, null, null);
                }
                LOGGER.debug(e, format("Error while executing JDBC query: \n%s", query));
                return QueryResult.errorResult(error, query);
            }

            return queryResult;
        }, QUERY_EXECUTOR);
    }

    @Override
    public QueryStats currentStats() {
        if (result.isDone()) {
            return new QueryStats(100, FINISHED, null, null, null, null, null, null);
        } else {
            return new QueryStats(null, RUNNING, null, null, null, null, null, null);
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
    public void kill() {
        if (statement != null && !update) {
            try {
                statement.cancel();
            } catch (SQLException e) {
                return;
            }
        }
    }

    private QueryResult resultSetToQueryResult(ResultSet resultSet, long executionTimeInMillis, Connection connection) {
        List<SchemaField> columns;
        List<List<Object>> data;
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            columns = new ArrayList<>(columnCount);
            for (int i = 1; i < columnCount + 1; i++) {
                FieldType type;
                try {
                    type = fromSql(metaData.getColumnType(i), metaData.getColumnTypeName(i));
                } catch (UnsupportedOperationException e) {
                    LOGGER.warn(e.getMessage());
                    type = STRING;
                }

                columns.add(new SchemaField(metaData.getColumnName(i), type));
            }

            ImmutableList.Builder<List<Object>> builder = ImmutableList.builder();
            while (resultSet.next()) {
                List<Object> rowBuilder = Arrays.asList(new Object[columnCount]);
                for (int i = 0; i < columnCount; i++) {
                    Object object;
                    SchemaField schemaField = columns.get(i);
                    if (schemaField == null) {
                        continue;
                    }
                    FieldType type = schemaField.getType();
                    int columnIndex = i + 1;
                    switch (type) {
                        case STRING:
                            object = resultSet.getString(columnIndex);
                            break;
                        case LONG:
                            object = resultSet.getLong(columnIndex);
                            break;
                        case INTEGER:
                            object = resultSet.getInt(columnIndex);
                            break;
                        case DECIMAL:
                            BigDecimal bigDecimal = resultSet.getBigDecimal(columnIndex);
                            object = bigDecimal != null ? bigDecimal.doubleValue() : null;
                            break;
                        case DOUBLE:
                            object = resultSet.getDouble(columnIndex);
                            break;
                        case BOOLEAN:
                            object = resultSet.getBoolean(columnIndex);
                            break;
                        case TIMESTAMP:
                            String timestamp = resultSet.getString(columnIndex);
                            if (zoneId != null && timestamp != null) {
                                object = connection.unwrap(PgConnection.class).getTimestampUtils().toLocalDateTime(timestamp).atZone(zoneId);
                            } else {
                                object = resultSet.getTimestamp(columnIndex);
                            }
                            break;
                        case DATE:
                            String string = resultSet.getString(columnIndex);
                            if (string != null) {
                                object = LocalDate.parse(string);
                            } else {
                                object = null;
                            }
                            break;
                        case TIME:
                            Time time = resultSet.getTime(columnIndex);
                            object = time != null ? time.toLocalTime() : null;
                            break;
                        case BINARY:
                            InputStream binaryStream = resultSet.getBinaryStream(columnIndex);
                            if (binaryStream != null) {
                                try {
                                    object = ByteStreams.toByteArray(binaryStream);
                                } catch (IOException e) {
                                    LOGGER.error("Error while de-serializing BINARY type", e);
                                    object = null;
                                }
                            } else {
                                object = null;
                            }
                            break;
                        default:
                            if (type.isArray()) {
                                Array array = resultSet.getArray(columnIndex);
                                object = array == null ? null : array.getArray();
                            } else if (type.isMap()) {
                                PGobject pgObject = (PGobject) resultSet.getObject(columnIndex);
                                if (pgObject == null) {
                                    object = null;
                                } else {
                                    if (pgObject.getType().equals("jsonb")) {
                                        object = JsonHelper.read(pgObject.getValue());
                                    } else {
                                        throw new UnsupportedOperationException("Postgresql type is not supported");
                                    }
                                }
                            } else {
                                throw new IllegalStateException();
                            }
                    }

                    if (resultSet.wasNull()) {
                        object = null;
                    }

                    rowBuilder.set(i, object);
                }
                builder.add(rowBuilder);
            }
            data = builder.build();

            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i) == null) {
                    columns.set(i, new SchemaField(metaData.getColumnName(i + 1), STRING));
                }
            }
            return new QueryResult(columns, data, ImmutableMap.of(EXECUTION_TIME, executionTimeInMillis, QUERY, query));
        } catch (SQLException e) {
            QueryError error = new QueryError(e.getMessage(), e.getSQLState(), e.getErrorCode(), null, null);
            return QueryResult.errorResult(error, query);
        }
    }
}
