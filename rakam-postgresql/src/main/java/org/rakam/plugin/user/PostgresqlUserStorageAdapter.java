package org.rakam.plugin.user;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.UserStorage;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;

import javax.inject.Inject;
import java.lang.reflect.ParameterizedType;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.rakam.analysis.postgresql.PostgresqlMetastore.fromSql;
import static org.rakam.realtime.AggregationType.COUNT;
import static org.rakam.util.ValidationUtil.*;

public class PostgresqlUserStorageAdapter implements UserStorage {
    public static final String USER_TABLE = "_users";
    public static final String PRIMARY_KEY = "id";
    private final PostgresqlQueryExecutor queryExecutor;
    private final Cache<String, Map<String, FieldType>> propertyCache = CacheBuilder.newBuilder().build();

    @Inject
    public PostgresqlUserStorageAdapter(PostgresqlQueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @Override
    public Object create(String project, Map<String, Object> properties) {
        checkProject(project);
        Object created = properties.get("created");
        if(created != null && !(created instanceof Instant)) {
            properties.remove("created");
        }
        Map<String, FieldType> columns = propertyCache.getIfPresent(project);
        if (columns == null) {
            columns = getProjectProperties(project);
        }

        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            FieldType fieldType = columns.get(entry.getKey());
            if (fieldType == null && entry.getValue() != null) {
                createColumn(project, entry.getKey(), entry.getValue());
                columns = getProjectProperties(project);
            }
        }

        try (Connection conn = queryExecutor.getConnection()) {
            StringBuilder cols = new StringBuilder();
            StringBuilder parametrizedValues = new StringBuilder();
            Iterator<String> stringIterator = properties.keySet().iterator();

            if (stringIterator.hasNext()) {
                String next = stringIterator.next();
                checkTableColumn(next, "unknown column");
                parametrizedValues.append("?");
                cols.append(next);
                while (stringIterator.hasNext()) {
                    next = stringIterator.next();
                    checkTableColumn(next, "unknown column");
                    cols.append(", ").append(next);
                    parametrizedValues.append(", ").append("?");
                }
            }

            parametrizedValues.append(", ").append("?");
            cols.append(", ").append("created");

            PreparedStatement statement = conn.prepareStatement("insert into  " + project + "." + USER_TABLE + " (" + cols +
                    ") values (" + parametrizedValues + ") RETURNING " + PRIMARY_KEY);
            int i = 1;
            for (Object o : properties.values()) {
                statement.setObject(i++, o);
            }
            statement.setTimestamp(i++, java.sql.Timestamp.from(Instant.now()));

            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            return resultSet.getObject(1);
        } catch (SQLException e) {
            // TODO: check error type
            throw Throwables.propagate(e);
        }
    }

    private void createColumn(String project, String column, Object value) {
        try (Connection conn = queryExecutor.getConnection()) {
            try {
                conn.createStatement().execute(format("alter table %s.%s add column %s %s",
                        project, USER_TABLE, column, getPostgresqlType(value.getClass())));
            } catch (SQLException e) {
                // TODO: check the column is already exists or this is a different exception
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public CompletableFuture<QueryResult> filter(String project, Expression filterExpression, List<EventFilter> eventFilter, Sorting sortColumn, long limit, long offset) {
        checkProject(project);
        List<SchemaField> projectColumns = getMetadata(project);
        String columns = Joiner.on(", ").join(projectColumns.stream().map(col -> col.getName())
                .toArray());

        LinkedList<String> filters = new LinkedList<>();
        if (filterExpression != null) {
            filters.add(new ExpressionFormatter.Formatter().process(filterExpression, null));
        }
        if (eventFilter != null && !eventFilter.isEmpty()) {
            for (EventFilter filter : eventFilter) {
                StringBuilder builder = new StringBuilder();

                checkCollection(filter.collection);
                if (filter.aggregation == null) {
                    builder.append(format("select \"_user\" from %s.%s", project, filter.collection));
                    if (filter.filterExpression != null) {
                        builder.append(" where ").append(new ExpressionFormatter.Formatter().process(filter.getExpression(), null));
                    }
                    filters.add((format("id in (%s)", builder.toString())));
                } else {
                    builder.append(format("select \"_user\" from %s.%s", project, filter.collection));
                    if (filter.filterExpression != null) {
                        builder.append(" where ").append(new ExpressionFormatter.Formatter().process(filter.getExpression(), null));
                    }
                    String field;
                    if (filter.aggregation.type == COUNT && filter.aggregation.field == null) {
                        field = "_user";
                    } else {
                        field = filter.aggregation.field;
                    }
                    builder.append(" group by \"_user\" ");
                    if(filter.aggregation.minimum != null || filter.aggregation.maximum != null) {
                        builder.append(" having ");
                    }
                    if (filter.aggregation.minimum != null) {
                        builder.append(format(" %s(\"%s\") >= %d ", filter.aggregation.type, field, filter.aggregation.minimum));
                    }
                    if (filter.aggregation.maximum != null) {
                        if(filter.aggregation.minimum != null) {
                            builder.append(" and ");
                        }
                        builder.append(format(" %s(\"%s\") < %d ", filter.aggregation.type, field, filter.aggregation.maximum));
                    }
                    filters.add((format("id in (%s)", builder.toString())));

                }
            }
        }

        if (sortColumn != null) {
            if (!projectColumns.stream().anyMatch(col -> col.getName().equals(sortColumn.column))) {
                throw new IllegalArgumentException(format("sorting column does not exist: %s", sortColumn.column));
            }
        }

        String orderBy = sortColumn == null ? "" : format(" ORDER BY %s %s", sortColumn.column, sortColumn.order);

        QueryExecution query = queryExecutor.executeRawQuery(format("SELECT %s FROM %s._users %s %s LIMIT %s OFFSET %s",
                columns, project, filters.isEmpty() ? "" : " WHERE " + Joiner.on(" AND ").join(filters), orderBy, limit, offset));

        CompletableFuture<QueryResult> dataResult = query.getResult();

        if (eventFilter == null || eventFilter.isEmpty()) {
            StringBuilder builder = new StringBuilder();
            builder.append(format("SELECT count(*) FROM %s.%s", project, USER_TABLE));
            if (filterExpression != null) {
                builder.append(" where ").append(filters.get(0));
            }

            QueryExecution totalResult = queryExecutor.executeRawQuery(builder.toString());

            CompletableFuture<QueryResult> result = new CompletableFuture<>();
            CompletableFuture.allOf(dataResult, totalResult.getResult()).whenComplete((__, ex) -> {
                QueryResult data = dataResult.join();
                QueryResult totalResultData = totalResult.getResult().join();
                if (ex == null && !data.isFailed() && !totalResultData.isFailed()) {
                    Object v1 = totalResultData.getResult().get(0).get(0);
                    result.complete(new QueryResult(projectColumns, data.getResult(), ImmutableMap.of(QueryResult.TOTAL_RESULT, v1)));
                } else {
                    result.complete(QueryResult.errorResult(new QueryError(ex.getMessage(), null, 0)));
                }
            });

            return result;
        } else {
            return dataResult;
        }
    }

    @Override
    public List<SchemaField> getMetadata(String project) {
        checkProject(project);
        LinkedList<SchemaField> columns = new LinkedList<>();

        try (Connection conn = queryExecutor.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet indexInfo = metaData.getIndexInfo(null, null, USER_TABLE, true, false);
            ResultSet dbColumns = metaData.getColumns(null, project, USER_TABLE, null);

            Set<String> uniqueColumns = Sets.newHashSet();
            while (indexInfo.next()) {
                uniqueColumns.add(indexInfo.getString("COLUMN_NAME"));
            }

            while (dbColumns.next()) {
                String columnName = dbColumns.getString("COLUMN_NAME");
                FieldType fieldType;
                try {
                    fieldType = fromSql(dbColumns.getInt("DATA_TYPE"));
                } catch (IllegalStateException e) {
                    continue;
                }
                columns.add(new SchemaField(columnName, fieldType, null, uniqueColumns.contains(columnName), null, null, null));
            }
            return columns;

        } catch (SQLException e) {
            throw new IllegalStateException("couldn't get metadata from plugin.user.storage");
        }
    }

    @Override
    public CompletableFuture<org.rakam.plugin.user.User> getUser(String project, String userId) {
        checkProject(project);

        return queryExecutor.executeRawQuery(format("select * from %s.%s where %s = %d", project, USER_TABLE, PRIMARY_KEY, Long.parseLong(userId)))
                .getResult().thenApply(result -> {
                    HashMap<String, Object> properties = Maps.newHashMap();
                    if (result.getResult().isEmpty()) {
                        return null;
                    }
                    List<Object> objects = result.getResult().get(0);
                    List<? extends SchemaField> metadata = result.getMetadata();

                    for (int i = 0; i < metadata.size(); i++) {
                        String name = metadata.get(i).getName();
                        if (!name.equals(PRIMARY_KEY))
                            properties.put(name, objects.get(i));
                    }

                    return new org.rakam.plugin.user.User(project, userId, properties);
                });
    }

    private Map<String, FieldType> getProjectProperties(String project) {
        Map<String, FieldType> columns = getMetadata(project).stream()
                .collect(Collectors.toMap(col -> col.getName(), col -> col.getType()));
        propertyCache.put(project, columns);
        return columns;
    }

    @Override
    public void setUserProperty(String project, String userId, String property, Object value) {
        checkProject(project);
        checkTableColumn(property, "user property");
        Map<String, FieldType> columns = propertyCache.getIfPresent(project);
        if (columns == null) {
            columns = getProjectProperties(project);
        }

        if (!columns.containsKey(property)) {
            createColumn(project, property, value);
        }

        try (Connection conn = queryExecutor.getConnection()) {
            PreparedStatement statement = conn.prepareStatement("update " + project + "." + USER_TABLE + " set " + property +
                    " = ? where " + PRIMARY_KEY + " = ?");
            statement.setObject(1, value);
            statement.setLong(2, Long.parseLong(userId));
            statement.executeUpdate();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    private String getPostgresqlType(Class clazz) {
        if (clazz.equals(String.class)) {
            return "text";
        } else if (clazz.equals(Float.class) || clazz.equals(Double.class)) {
            return "bool";
        } else if (Number.class.isAssignableFrom(clazz)) {
            return "bigint";
        } else if (clazz.equals(Boolean.class)) {
            return "bool";
        } else if (Collection.class.isAssignableFrom(clazz)) {
            return getPostgresqlType((Class) ((ParameterizedType) getClass().getGenericInterfaces()[0]).getActualTypeArguments()[0]) + "[]";
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void createProject(String project) {
        checkProject(project);
        queryExecutor.executeRawQuery(format("CREATE TABLE IF NOT EXISTS %s.%s (" +
                "  %s SERIAL NOT NULL,\n" +
                "  created timestamp NOT NULL,\n" +
                "  PRIMARY KEY (%s)" +
                ")", project, USER_TABLE, PRIMARY_KEY, PRIMARY_KEY));
    }

    @Override
    public boolean isEventFilterSupported() {
        return true;
    }
}
