package org.rakam.plugin.user.impl;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.JDBCConfig;
import org.rakam.plugin.UserPluginConfig;
import org.rakam.plugin.UserStorage;
import org.rakam.plugin.user.User;
import org.rakam.report.QueryError;
import org.rakam.report.QueryResult;
import org.rakam.util.NotImplementedException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.util.LongMapper;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:46.
 */
@Singleton
public class JDBCUserStorageAdapter implements UserStorage {
    final static Logger LOGGER = Logger.get(JDBCUserStorageAdapter.class);

    private final JDBCUserStorageConfig config;
    private final UserPluginConfig moduleConfig;
    private final JDBCConfig jdbcConfig;
    private final LoadingCache<String, List<SchemaField>> metadataCache;
    Handle dao;

    @Inject
    public JDBCUserStorageAdapter(@Named("plugin.user.storage.jdbc") JDBCConfig jdbcConfig, JDBCUserStorageConfig config, UserPluginConfig moduleConfig) {
        this.config = config;
        this.moduleConfig = moduleConfig;
        this.jdbcConfig = jdbcConfig;

        DBI dbi = new DBI(() -> getConnection());
        dao = dbi.open();
        metadataCache = CacheBuilder.newBuilder().build(new CacheLoader<String, List<SchemaField>>() {
            @Override
            public List<SchemaField> load(String key) throws Exception {
                return getMetadataInternal(key);
            }
        });
    }

    private Connection getConnection() {
        try {
            return DriverManager.getConnection(jdbcConfig.getUrl(), jdbcConfig.getUsername(), jdbcConfig.getPassword());
        } catch (SQLException e) {
            throw new IllegalStateException(String.format("couldn't connect user storage using jdbc connection %s", jdbcConfig.getUrl()));
        }
    }

    @Override
    public Object create(String project, Map<String, Object> properties) {
        throw new NotImplementedException();
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
    public CompletableFuture<QueryResult> filter(String project, Expression filterExpression, List<EventFilter> eventFilter, Sorting sortColumn, long limit, long offset) {
        String table = config.getMappings().get(project);
        if(table == null) {
            throw new IllegalArgumentException("Project is not defined in mapping definition");
        }
        if(eventFilter != null && !eventFilter.isEmpty()) {
            // TODO: we may consider querying events that returns ids of users with QueryExecutor and querying users with those ids.
            throw new IllegalArgumentException("Event store adapter is not set");
        }

        //  ExpressionFormatter is not the best way to do this but the basic expressions will work for all the SQL databases.
        // TODO: implement functions specific to sql databases.
        String filterQuery;
        if (filterExpression != null) {
            filterQuery = new ExpressionFormatter.Formatter().process(filterExpression, null);
        } else {
            filterQuery = null;
        }

        List<SchemaField> projectColumns = getMetadata(project);
        String columns = Joiner.on(", ").join(projectColumns.stream().map(col -> col.getName()).toArray());
        String where = filterQuery == null || filterQuery.isEmpty() ? "" : " WHERE " + filterQuery;

        if(sortColumn != null) {
            if (!projectColumns.stream().anyMatch(col -> col.getName().equals(sortColumn.column))) {
                throw new IllegalArgumentException(format("sorting column does not exist: %s", sortColumn.column));
            }
        }

        String orderBy = sortColumn == null ? "" : format(" ORDER BY %s %s", sortColumn.column, sortColumn.order);

        String query = String.format("SELECT %s FROM %s %s %s LIMIT %s OFFSET %s", columns, table, where, orderBy, limit, offset);

        CompletableFuture<List<List<Object>>> data = CompletableFuture.supplyAsync(() ->
                dao.createQuery(query).map((i, resultSet, statementContext) -> {
                    List<SchemaField> schemaFields = getMetadata(project);

                    return IntStream.range(1, schemaFields.size()+1).mapToObj(idx -> {
                        try {
                            return resultSet.getObject(idx);
                        } catch (SQLException e) {
                            throw Throwables.propagate(e);
                        }
                    }).collect(Collectors.toList());

                }).list());

        CompletableFuture<Long> totalResult = CompletableFuture.supplyAsync(() ->
                dao.createQuery(String.format("SELECT count(*) FROM %s %s", table, where))
                        .map(new LongMapper(1)).first());

        CompletableFuture<QueryResult> result = new CompletableFuture<>();

        CompletableFuture.allOf(data, totalResult).whenComplete((__, ex) -> {
            if(ex==null) {
                result.complete(new QueryResult(projectColumns, data.join(), ImmutableMap.of("totalResult", totalResult.join())));
            } else {
                LOGGER.error("Error while executing query on user data-set.", ex);
                result.complete(QueryResult.errorResult(new QueryError(ex.getMessage(), null, 0)));
            }
        });

        return result;
    }

    @Override
    public List<SchemaField> getMetadata(String project) {
        if(!config.getMappings().containsKey(project)) {
            throw new IllegalArgumentException();
        }
        try {
            return metadataCache.get(project);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    public List<SchemaField> getMetadataInternal(String project) {
        String table = config.getMappings().get(project);
        LinkedList<SchemaField> columns = new LinkedList<>();

        try(Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            String[] schemaTable = table.split("\\.", 2);
            String schema = null;
            if(schemaTable.length > 1) {
                schema = schemaTable[0];
                table = schemaTable[1];
            }
            ResultSet indexInfo = metaData.getIndexInfo(null, schema, table, true, false);
            ResultSet dbColumns = metaData.getColumns(null, schema, table, null);

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
                columns.add(new SchemaField(columnName, fieldType, null, uniqueColumns.indexOf(columnName) > -1, null, null, null));
            }

        } catch (SQLException e) {
            throw new IllegalStateException("couldn't get metadata from plugin.user.storage");
        }

        return columns;
    }

    @Override
    public CompletableFuture<User> getUser(String project, String userId) {
        String columns = Joiner.on(", ").join(getMetadata(project).stream().map(col -> col.getName()).toArray());

        // TODO: fix
        return CompletableFuture.completedFuture(dao.createQuery(String.format("SELECT %s FROM %s WHERE %s = %s",
                columns,
                config.getMappings().get(project),
                moduleConfig.getIdentifierColumn(),
                userId)).map((index, r, ctx) -> {
            HashMap<String, Object> properties = new HashMap<>();
            for (SchemaField column : getMetadata(project)) {
                properties.put(column.getName(), r.getObject(column.getName()));
            }
            return new User(project, userId, properties);
        }).first());
    }

    @Override
    public void setUserProperty(String project, String user, String property, Object value) {
        Optional<SchemaField> any = getMetadata(project).stream().filter(column -> column.getName().equals(property)).findAny();
        if(!any.isPresent()) {
            throw new NoSuchElementException("column is not exist");
        }
        SchemaField column = any.get();

        Update statement = dao.createStatement(String.format("UPDATE %s SET %s = :value WHERE %s = %s",
                config.getMappings().get(project),
                property,
                moduleConfig.getIdentifierColumn(),
                user));

        switch (column.getType()) {
            case DATE:
                if(value instanceof CharSequence) {
                    Instant parse = Instant.parse((CharSequence) value);
                    statement.bind("value", java.sql.Date.from(parse));
                    break;
                }
            case TIME:
                if(value instanceof CharSequence) {
                    Instant parse = Instant.parse((CharSequence) value);
                    statement.bind("value", java.sql.Time.from(parse));
                    break;
                }
            case LONG:
                if(value instanceof Number) {
                    statement.bind("value", ((Number) value).longValue());
                    break;
                }
            case DOUBLE:
                if(value instanceof Number) {
                    statement.bind("value", ((Number) value).doubleValue());
                    break;
                }
            case STRING:
                statement.bind("value", value.toString());
                break;
            case BOOLEAN:
                if(value instanceof Boolean) {
                    statement.bind("value", value);
                    break;
                }
            default:
                throw new UnsupportedOperationException();
        }

        statement.execute();
    }

    @Override
    public void createProject(String project) {
        throw new NotImplementedException();
    }

    @Override
    public boolean isEventFilterSupported() {
        return false;
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
                return FieldType.TIME;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return FieldType.TIMESTAMP;
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.VARCHAR:
                return FieldType.STRING;
            default:
                throw new IllegalStateException("sql type couldn't converted to fieldtype");
        }
    }

}
