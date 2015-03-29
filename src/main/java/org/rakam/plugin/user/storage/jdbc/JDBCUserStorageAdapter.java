package org.rakam.plugin.user.storage.jdbc;

import com.facebook.presto.jdbc.internal.guava.base.Throwables;
import com.facebook.presto.jdbc.internal.guava.collect.Lists;
import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.rakam.collection.FieldType;
import org.rakam.plugin.user.storage.Column;
import org.rakam.plugin.user.storage.UserStorage;
import org.rakam.report.QueryError;
import org.rakam.report.QueryResult;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.LongMapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:46.
 */
@Singleton
public class JDBCUserStorageAdapter implements UserStorage {

    private final JDBCUserStorageConfig config;
    private List<Column> metadata;
    Handle dao;

    ResultSetMapper<List<Object>> mapper = new ResultSetMapper<List<Object>>() {
        @Override
        public List<Object> map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            List<Object> row = new ArrayList<>(metadata.size());
            for (int i = 0; i < metadata.size(); i++) {
                row.add(r.getObject(i + 1));
            }
            return row;
        }
    };

    @Inject
    public JDBCUserStorageAdapter(JDBCUserStorageConfig config) {
        this.config = config;

        DBI dbi = new DBI(() -> getConnection());
        dao = dbi.open();
    }

    private Connection getConnection() {
        try {
            return DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
        } catch (SQLException e) {
            throw new IllegalStateException(format("couldn't connect user storage using jdbc connection %s", config.getUrl()));
        }
    }

    @Override
    public void create(String project, Map<String, Object> properties) {
        //session.save("Users", properties);
    }

    public static Object[] all(Supplier... futures) {
        Object[] objects = new Object[futures.length];
        CompletableFuture.allOf(
                IntStream.range(0, futures.length)
                        .mapToObj(i -> CompletableFuture.supplyAsync(futures[i]).thenAccept((data) -> objects[i] = data))
                        .toArray(CompletableFuture[]::new)).join();
        return objects;
    }

    @Override
    public QueryResult filter(String project, Expression filterExpression, int limit, int offset) {
        //  ExpressionFormatter is not the best way to do this but the basic expressions will work for all the SQL databases.
        // TODO: implement functions specific to sql databases.
        String filterQuery;
        if(filterExpression != null) {
            filterQuery = new ExpressionFormatter.Formatter().process(filterExpression, null);
        }else {
            filterQuery = null;
        }

        String columns = Joiner.on(", ").join(getMetadata(project).stream().map(col -> col.getName()).toArray());
        String where = filterQuery == null || filterQuery.isEmpty() ? "" : " WHERE " + filterQuery;

        CompletableFuture<List<List<Object>>> data = CompletableFuture.supplyAsync(() ->
                    dao.createQuery(format("SELECT %s FROM %s %s LIMIT %s OFFSET %s",
                            columns,
                            config.getTable(),
                            where,
                            limit,
                            offset)).map(mapper).list());

        CompletableFuture<Long> totalResult = CompletableFuture.supplyAsync(() ->
                dao.createQuery(format("SELECT count(*) FROM %s %s", config.getTable(), where))
                        .map(new LongMapper(1)).first());

        List<List<Object>> dataJoin;
        Long totalResultJoin;
        // TODO: shame on me.
        try {
            dataJoin = data.join();
            totalResultJoin = totalResult.join();
        } catch (CompletionException e) {
            if(e.getCause() instanceof UnableToExecuteStatementException) {
                return new QueryResult(null, null, null, new QueryError(e.getMessage(), null, 0));
            } else {
                throw Throwables.propagate(e);
            }
        }

        return new QueryResult(getMetadata(project), dataJoin, new QueryResult.Stat(totalResultJoin), null);
    }

    @Override
    public List<Column> getMetadata(String project) {

        if (metadata == null) {
            Connection conn = getConnection();
            LinkedList<Column> columns = new LinkedList<>();

            try {
                DatabaseMetaData metaData = conn.getMetaData();
                ResultSet indexInfo = metaData.getIndexInfo(null, null, config.getTable(), true, false);
                ResultSet dbColumns = metaData.getColumns(null, null, config.getTable(), null);

                List<String> uniqueColumns = Lists.newLinkedList();
                while (indexInfo.next()) {
                    uniqueColumns.add(indexInfo.getString("COLUMN_NAME"));
                }

                while (dbColumns.next()) {
                    String columnName = dbColumns.getString("COLUMN_NAME");
                    if (config.getColumns() != null && config.getColumns().indexOf(columnName) == -1) {
                        continue;
                    }
                    FieldType fieldType;
                    try {
                        fieldType = fromSql(dbColumns.getInt("DATA_TYPE"));
                    } catch (IllegalStateException e) {
                        continue;
                    }
                    columns.add(new Column(columnName, fieldType, uniqueColumns.indexOf(columnName) > -1));
                }

            } catch (SQLException e) {
                throw new IllegalStateException("couldn't get metadata from plugin.user.storage");
            }

            metadata = columns;
        }
        return metadata;
    }

    static FieldType fromSql(int sqlType) {
        switch (sqlType) {
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.BIGINT:
                return FieldType.LONG;
            case Types.TINYINT:
            case Types.NUMERIC:
            case Types.INTEGER:
            case Types.SMALLINT:
                return FieldType.LONG;
            case Types.BOOLEAN:
            case Types.BIT:
                return FieldType.BOOLEAN;
            case Types.DATE:
                return FieldType.DATE;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return FieldType.TIME;
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.VARCHAR:
                return FieldType.STRING;
            default:
                throw new IllegalStateException("sql type couldn't converted to fieldtype");
        }
    }

}
