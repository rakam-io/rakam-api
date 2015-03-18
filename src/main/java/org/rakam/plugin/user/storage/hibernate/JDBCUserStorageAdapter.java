package org.rakam.plugin.user.storage.hibernate;

import com.facebook.presto.jdbc.internal.guava.collect.Lists;
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.rakam.collection.FieldType;
import org.rakam.plugin.user.FilterCriteria;
import org.rakam.plugin.user.storage.Column;
import org.rakam.plugin.user.storage.UserStorage;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.report.QueryStats;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
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
    public QueryResult filter(String project, List<FilterCriteria> filters, int limit, int offset) {
        String columns = Joiner.on(", ").join(getMetadata(project).stream().map(col -> col.getName()).toArray());

        CompletableFuture<List<List<Object>>> data = CompletableFuture.supplyAsync(() -> dao.createQuery(format("SELECT %s FROM %s LIMIT %s OFFSET %s",
                columns, config.getTable(), limit, offset)).map(mapper).list());

        CompletableFuture<Long> totalResult = CompletableFuture.supplyAsync(() -> dao.createQuery(format("SELECT count(*) FROM %s", config.getTable())).map(new LongMapper(1)).first());

        return new QueryResult(getMetadata(project), data.join(), new QueryResult.Stat(totalResult.join()), null);
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

    private class JDBCQueryExecution implements QueryExecution {
        private final CompletableFuture<Object[]> users;
        private final String project;

        public JDBCQueryExecution(CompletableFuture<Object[]> users, String project) {
            this.users = users;
            this.project = project;
        }

        @Override
        public QueryStats currentStats() {
            if (users.isDone()) {
                return new QueryStats(100, "FINISHED", 0, 0, 0, 0, 0, 0);
            } else {
                return new QueryStats(0, "PROCESSING", 0, 0, 0, 0, 0, 0);
            }
        }

        @Override
        public boolean isFinished() {
            return users.isDone();
        }

        @Override
        public CompletableFuture<QueryResult> getResult() {
            return users.thenApply(data -> new QueryResult(getMetadata(project),
                    (List<List<Object>>) data[0],
                    new QueryResult.Stat((Long) data[1]), null));
        }
    }
}
