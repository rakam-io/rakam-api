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
import org.rakam.analysis.postgresql.PostgresqlMetastore;
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
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
    public PostgresqlUserStorageAdapter(PostgresqlQueryExecutor queryExecutor, PostgresqlMetastore metastore) {
        this.queryExecutor = queryExecutor;
        metastore.getProjects().forEach(this::createProject);
    }

    @Override
    public String create(String project, Map<String, Object> properties) {
        createMissingColumns(project, properties);
        return createInternal(project, properties);
    }

    private void createMissingColumns(String project, Map<String, Object> properties) {
        Map<String, FieldType> columns = propertyCache.getIfPresent(project);
        if (columns == null) {
            columns = getProjectProperties(project);
        }

        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            FieldType fieldType = columns.get(entry.getKey());
            if (fieldType == null && entry.getValue() != null) {
                checkTableColumn(entry.getKey(), String.format("user property '%s' is invalid.", entry.getKey()));
                createColumn(project, entry.getKey(), entry.getValue());
                columns = getProjectProperties(project);
            }
        }
    }

    public String createInternal(String project, Map<String, Object> properties) {
        try (Connection conn = queryExecutor.getConnection()) {
            checkProject(project);
            Object created = properties.remove("created_at");
            Object id = properties.remove("id");
            Instant createdAt;
            String userId;

            if (id != null) {
                userId = id.toString();
            } else {
                userId = UUID.randomUUID().toString();
            }

            if (created instanceof String) {
                try {
                    createdAt = Instant.parse((String) created);
                } catch (Exception e) {
                    createdAt = Instant.now();
                }
            } else {
                createdAt = Instant.now();
            }

            Map<String, FieldType> columns = propertyCache.getIfPresent(project);

            StringBuilder cols = new StringBuilder();
            StringBuilder parametrizedValues = new StringBuilder();
            Iterator<String> stringIterator = properties.keySet().iterator();

            if (stringIterator.hasNext()) {
                String next = stringIterator.next();
                parametrizedValues.append("?");
                cols.append(next);
                while (stringIterator.hasNext()) {
                    next = stringIterator.next();
                    cols.append(", ").append(next);
                    parametrizedValues.append(", ").append("?");
                }
            }

            parametrizedValues.append(", ").append("?");
            cols.append(", ").append("created_at");

            parametrizedValues.append(", ").append("?");
            cols.append(", ").append("id");

            PreparedStatement statement = conn.prepareStatement("INSERT INTO  " + project + "." + USER_TABLE + " (" + cols +
                    ") values (" + parametrizedValues + ") RETURNING " + PRIMARY_KEY);
            int i = 1;
            for (Map.Entry<String, Object> o : properties.entrySet()) {
                FieldType fieldType = columns.get(o.getKey());
                if (fieldType.isArray()) {
                    throw new UnsupportedOperationException("array type is not supported yet");
                }

                Object jdbcValue = getJDBCValue(fieldType, o.getValue(), conn);
                statement.setObject(i++, jdbcValue);
            }
            statement.setTimestamp(i++, java.sql.Timestamp.from(createdAt));
            statement.setString(i++, userId);

            ResultSet resultSet;
            try {
                resultSet = statement.executeQuery();
            } catch (SQLException e) {
                if(e.getMessage().contains("duplicate key value")) {
                    return userId;
                } else {
                    throw e;
                }
            }
            resultSet.next();
            return resultSet.getString(1);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    private String sqlArrayTypeName(FieldType fieldType) {
        if(fieldType.isArray()) {
            throw new UnsupportedOperationException();
        }
        switch (fieldType) {
            case BOOLEAN: return "boolean";
            case STRING: return "varchar";
            case DOUBLE: return "double precision";
            case LONG: return "bigint";
            case TIMESTAMP: return "timestamp";
            case TIME: return "time";
            case DATE: return "date";
            default: throw new UnsupportedOperationException();
        }
    }

    public Object getJDBCValue(FieldType fieldType, Object value, Connection conn) throws SQLException {
        if(fieldType.isArray()) {
            if(value instanceof List) {
                FieldType arrayType = fieldType.getArrayType();
                List value1 = (List) value;

                Object[] objects = new Object[value1.size()];
                for (int i = 0; i < value1.size(); i++) {
                    objects[i] = getJDBCValue(arrayType, value1.get(i), conn);
                }
                return conn.createArrayOf(sqlArrayTypeName(arrayType), objects);
            } else {
                return null;
            }
        }
        switch (fieldType) {
            case TIMESTAMP:
            case DATE:
               return parseTimestamp(value);
            case LONG:
            case DOUBLE:
                return value instanceof Number ? value : null;
            case STRING:
                return value instanceof String ? value : null;
            case TIME:
                return parseTime(value);
            case BOOLEAN:
                return value instanceof Boolean ? value : null;
            default:
                throw new UnsupportedOperationException();

        }
    }

    private Time parseTime(Object value) {
        if(value instanceof String) {
            try {
                return Time.valueOf((String) value);
            } catch (Exception e) {
                return null;
            }
        } else {
            return null;
        }
    }
    private Timestamp parseTimestamp(Object value) {
        if(value instanceof String) {
            try {
                return Timestamp.from(Instant.parse((CharSequence) value));
            } catch (Exception e) {
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public List<String> batchCreate(String project, List<org.rakam.plugin.user.User> users) {
        Map<String, Object> props = new HashMap<>();
        for (User user : users) {
            props.putAll(user.properties);
        }
        createMissingColumns(project, props);

        // may use transaction when we start to use Postgresql 9.5. Since we use insert or merge, it doesn't work right now.
        return users.stream()
                .map(user -> create(project, user.properties))
                .collect(Collectors.toList());
    }

    private void createColumn(String project, String column, Object value) {
        checkTableColumn(column, "user property name is not valid");
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
                    if (filter.aggregation.minimum != null || filter.aggregation.maximum != null) {
                        builder.append(" having ");
                    }
                    if (filter.aggregation.minimum != null) {
                        builder.append(format(" %s(\"%s\") >= %d ", filter.aggregation.type, field, filter.aggregation.minimum));
                    }
                    if (filter.aggregation.maximum != null) {
                        if (filter.aggregation.minimum != null) {
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
    public void setUserProperty(String project, String userId, Map<String, Object> properties) {
        setUserProperty(project, userId, properties, false);
    }

    @Override
    public void setUserPropertyOnce(String project, String userId, Map<String, Object> properties) {
        setUserProperty(project, userId, properties, true);
    }

    public void setUserProperty(String project, String userId, Map<String, Object> properties, boolean onlyOnce) {
        checkProject(project);
        Map<String, FieldType> columns = propertyCache.getIfPresent(project);
        if (columns == null) {
            columns = getProjectProperties(project);
        }

        StringBuilder builder = new StringBuilder("update " + project + "." + USER_TABLE + " set ");
        Iterator<Map.Entry<String, Object>> entries = properties.entrySet().iterator();
        if (entries.hasNext()) {
            Map.Entry<String, Object> entry = entries.next();
            FieldType fieldType = columns.get(entry);
            if (fieldType == null) {
                createColumn(project, entry.getKey(), entry.getValue());
            }
            builder.append("\"").append(entry.getKey())
                    .append(onlyOnce ? "\"=coalesce(\""+entry.getKey()+"\", ?)" : "\"=?");

            while (entries.hasNext()) {
                entry = entries.next();
                fieldType = columns.get(entry);
                if (fieldType == null) {
                    createColumn(project, entry.getKey(), entry.getValue());
                }
                builder.append(" and ").append("\"").append(entry.getKey())
                        .append(onlyOnce ? "\"=coalesce(\""+entry.getKey()+"\", ?)" : "\"=?");
            }
        }

        builder.append(" where " + PRIMARY_KEY + " = ?");

        try (Connection conn = queryExecutor.getConnection()) {
            PreparedStatement statement = conn.prepareStatement(builder.toString());
            int i = 1;
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                FieldType fieldType = columns.get(entry);
                statement.setObject(i++, getJDBCValue(fieldType, entry.getValue(), conn));
            }
            statement.setLong(i++, Long.parseLong(userId));

            i = statement.executeUpdate();
            if(i == 0) {
                Map<String, Object> map = new HashMap<>(properties.size()+1);
                map.putAll(properties);
                map.put(PRIMARY_KEY, userId);
                create(project, map);
            }
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
        queryExecutor.executeRawStatement(format("CREATE TABLE IF NOT EXISTS %s.%s (" +
                "  %s TEXT NOT NULL,\n" +
                "  created_at timestamp NOT NULL,\n" +
                "  PRIMARY KEY (%s)" +
                ")", project, USER_TABLE, PRIMARY_KEY, PRIMARY_KEY));
    }
}
