package org.rakam.plugin.user;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.postgresql.util.PGobject;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.UserStorage;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import java.lang.reflect.ParameterizedType;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.rakam.analysis.postgresql.PostgresqlMetastore.fromSql;
import static org.rakam.util.ValidationUtil.checkProject;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public abstract class AbstractPostgresqlUserStorage implements UserStorage {
    private final PostgresqlQueryExecutor queryExecutor;
    private final Cache<String, Map<String, FieldType>> propertyCache = CacheBuilder.newBuilder().build();

    public AbstractPostgresqlUserStorage(PostgresqlQueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @Override
    public String create(String project, String id, Map<String, Object> properties) {
        Set<Map.Entry<String, Object>> props = properties.entrySet();

        createMissingColumns(project, props);
        return createInternal(project, id, props);
    }

    private Map<String, FieldType> createMissingColumns(String project, Iterable<Map.Entry<String, Object>> properties) {
        Map<String, FieldType> columns = propertyCache.getIfPresent(project);
        if (columns == null) {
            columns = getProjectProperties(project);
        }

        for (Map.Entry<String, Object> entry : properties) {
            FieldType fieldType = columns.get(entry.getKey());
            if (fieldType == null && entry.getValue() != null) {
                checkTableColumn(entry.getKey(), String.format("user property '%s' is invalid.", entry.getKey()));
                createColumn(project, entry.getKey(), entry.getValue());
                columns = getProjectProperties(project);
            }
        }

        return columns;
    }

    public String createInternal(String project, String id, Iterable<Map.Entry<String, Object>> properties) {
        try (Connection conn = queryExecutor.getConnection()) {
            checkProject(project);

            Map<String, FieldType> columns = propertyCache.getIfPresent(project);

            StringBuilder cols = new StringBuilder();
            StringBuilder parametrizedValues = new StringBuilder();
            Iterator<Map.Entry<String, Object>> stringIterator = properties.iterator();

            if (stringIterator.hasNext()) {
                Map.Entry<String, Object> next = stringIterator.next();

                if(!next.getKey().equals(PRIMARY_KEY) && !next.getKey().equals("created_at")) {
                    parametrizedValues.append("?");
                    cols.append(next.getKey());
                }

                while (stringIterator.hasNext()) {
                    next = stringIterator.next();

                    if(!next.getKey().equals(PRIMARY_KEY) && !next.getKey().equals("created_at")) {
                        cols.append(", ").append(next.getKey());
                        parametrizedValues.append(", ").append("?");
                    }
                }
            }

            parametrizedValues.append(", ").append("?");
            cols.append(", ").append("created_at");

            parametrizedValues.append(", ").append("?");
            cols.append(", ").append(PRIMARY_KEY);

            PreparedStatement statement = conn.prepareStatement("INSERT INTO  " + getUserTable(project) + " (" + cols +
                    ") values (" + parametrizedValues + ") RETURNING " + PRIMARY_KEY);

            Instant createdAt = null;
            int i = 1;
            for (Map.Entry<String, Object> o : properties) {
                if(o.getKey().equals(PRIMARY_KEY)) {
                    throw new RakamException(String.format("User property %s is invalid. It's used as primary key", PRIMARY_KEY), HttpResponseStatus.BAD_REQUEST);
                }

                if(o.getKey().equals("created_at")) {
                    if(o.getValue() instanceof String) {
                        createdAt = Instant.parse(o.getValue().toString());
                    }
                } else {
                    FieldType fieldType = columns.get(o.getKey());
                    statement.setObject(i++, getJDBCValue(fieldType, o.getValue(), conn));
                }
            }

            statement.setTimestamp(i++, java.sql.Timestamp.from(createdAt == null ? Instant.now() :createdAt));
            statement.setString(i++, id);

            ResultSet resultSet;
            try {
                resultSet = statement.executeQuery();
            } catch (SQLException e) {
                if (e.getMessage().contains("duplicate key value")) {
                    return id;
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
        if (fieldType.isArray()) {
            throw new UnsupportedOperationException();
        }
        switch (fieldType) {
            case BOOLEAN:
                return "boolean";
            case STRING:
                return "varchar";
            case DOUBLE:
                return "double precision";
            case LONG:
                return "bigint";
            case TIMESTAMP:
                return "timestamp";
            case TIME:
                return "time";
            case DATE:
                return "date";
            default:
                throw new UnsupportedOperationException();
        }
    }

    public Object getJDBCValue(FieldType fieldType, Object value, Connection conn) throws SQLException {
        if (fieldType.isArray()) {
            if (value instanceof List) {
                FieldType arrayType = fieldType.getArrayElementType();
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
        if (fieldType.isMap()) {
            if (value instanceof Map) {
                PGobject jsonObject = new PGobject();
                jsonObject.setType("jsonb");
                jsonObject.setValue(JsonHelper.encode(value));
                return jsonObject;
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
        if (value instanceof String) {
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
        if (value instanceof String) {
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
        createMissingColumns(project, props.entrySet());

        // may use transaction when we start to use Postgresql 9.5. Since we use insert or merge, it doesn't work right now.
        return users.stream()
                .map(user -> create(project, user.id, user.properties))
                .collect(Collectors.toList());
    }

    private void createColumn(String project, String column, Object value) {
        // it must be called from a separated transaction, otherwise it may lock table and the other insert may cause deadlock.
        checkTableColumn(column, "user property name is not valid");
        try (Connection conn = queryExecutor.getConnection()) {
            try {
                conn.createStatement().execute(format("alter table %s add column %s %s",
                        getUserTable(project), column, getPostgresqlType(value.getClass())));
            } catch (SQLException e) {
                // TODO: check the column is already exists or this is a different exception
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public abstract List<String> getEventFilterPredicate(String project, List<EventFilter> eventFilter);

    @Override
    public CompletableFuture<QueryResult> filter(String project, List<String> selectColumns, Expression filterExpression, List<EventFilter> eventFilter, Sorting sortColumn, long limit, String offset) {
        checkProject(project);
        List<SchemaField> metadata = getMetadata(project);
        Stream<SchemaField> projectColumns = metadata.stream();
        if (selectColumns != null) {
            projectColumns = projectColumns.filter(column -> selectColumns.contains(column.getName()));
        }
        // TODO: fail id column is not exist.
        String columns = Joiner.on(", ").join(projectColumns.map(col -> col.getName())
                .toArray());

        LinkedList<String> filters = new LinkedList<>();
        if (filterExpression != null) {
            filters.add(new ExpressionFormatter.Formatter().process(filterExpression, true));
        }

        if (eventFilter != null && !eventFilter.isEmpty()) {
            filters.addAll(getEventFilterPredicate(project, eventFilter));
        }

        if (sortColumn != null) {
            if (!metadata.stream().anyMatch(col -> col.getName().equals(sortColumn.column))) {
                throw new IllegalArgumentException(format("sorting column does not exist: %s", sortColumn.column));
            }
        }

        String orderBy = sortColumn == null ? "" : format(" ORDER BY %s %s", sortColumn.column, sortColumn.order);

        QueryExecution query = queryExecutor.executeRawQuery(format("SELECT %s FROM %s %s %s LIMIT %s OFFSET %s",
                columns, getUserTable(project), filters.isEmpty() ? "" : " WHERE " + Joiner.on(" AND ").join(filters), orderBy, limit, offset));

        CompletableFuture<QueryResult> dataResult = query.getResult();

        if (eventFilter == null || eventFilter.isEmpty()) {
            StringBuilder builder = new StringBuilder();
            builder.append("SELECT count(*) FROM " + getUserTable(project));
            if (filterExpression != null) {
                builder.append(" WHERE ").append(filters.get(0));
            }

            QueryExecution totalResult = queryExecutor.executeRawQuery(builder.toString());

            CompletableFuture<QueryResult> result = new CompletableFuture<>();
            CompletableFuture.allOf(dataResult, totalResult.getResult()).whenComplete((__, ex) -> {
                QueryResult data = dataResult.join();
                QueryResult totalResultData = totalResult.getResult().join();
                if (ex == null && !data.isFailed() && !totalResultData.isFailed()) {
                    Object v1 = totalResultData.getResult().get(0).get(0);
                    result.complete(new QueryResult(data.getMetadata(), data.getResult(),
                            ImmutableMap.of(QueryResult.TOTAL_RESULT, v1)));
                } else if (ex != null) {
                    result.complete(QueryResult.errorResult(new QueryError(ex.getMessage(), null, 0, null, null)));
                } else {
                    result.complete(data);
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
            String[] userTable = getUserTable(project).split("\\.", 2);
            ResultSet indexInfo = metaData.getIndexInfo(null, userTable[0], userTable[1], true, false);
            ResultSet dbColumns = metaData.getColumns(null, userTable[0], userTable[1], null);

            Set<String> uniqueColumns = Sets.newHashSet();
            while (indexInfo.next()) {
                uniqueColumns.add(indexInfo.getString("COLUMN_NAME"));
            }

            while (dbColumns.next()) {
                String columnName = dbColumns.getString("COLUMN_NAME");
                FieldType fieldType;
                try {
                    fieldType = fromSql(dbColumns.getInt("DATA_TYPE"), dbColumns.getString("TYPE_NAME"));
                } catch (IllegalStateException e) {
                    continue;
                }
                columns.add(new SchemaField(columnName, fieldType, uniqueColumns.contains(columnName), null, null, null));
            }
            return columns;

        } catch (SQLException e) {
            throw new IllegalStateException("couldn't get metadata from plugin.user.storage");
        }
    }

    public abstract String getUserTable(String project);

    @Override
    public CompletableFuture<org.rakam.plugin.user.User> getUser(String project, String userId) {
        checkProject(project);
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = queryExecutor.getConnection()) {
                PreparedStatement ps = conn.prepareStatement(format("select * from %s where %s = ?", getUserTable(project), PRIMARY_KEY));
                ps.setString(1, userId);
                ResultSet resultSet = ps.executeQuery();

                Map<String, Object> properties = new HashMap<>();
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount() + 1;

                String id = null;
                while (resultSet.next()) {
                    for (int i = 1; i < columnCount; i++) {
                        String key = metaData.getColumnName(i);
                        if (key.equals(PRIMARY_KEY)) {
                            id = resultSet.getString(i);
                        } else {
                            Object value = resultSet.getObject(i);
                            properties.put(key, value);
                        }
                    }
                }
                return new User(project, id, null, properties);
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }
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
        setUserProperty(project, userId, properties.entrySet(), false);
    }

    @Override
    public void setUserPropertyOnce(String project, String userId, Map<String, Object> properties) {
        setUserProperty(project, userId, properties.entrySet(), true);
    }

    public void setUserProperty(String project, String userId, Iterable<Map.Entry<String, Object>> properties, boolean onlyOnce) {
        Map<String, FieldType> columns = propertyCache.getIfPresent(project);
        if (columns == null) {
            columns = createMissingColumns(project, properties);
        }

        StringBuilder builder = new StringBuilder("update " + getUserTable(project) + " set ");
        Iterator<Map.Entry<String, Object>> entries = properties.iterator();
        if (entries.hasNext()) {
            Map.Entry<String, Object> entry = entries.next();
            builder.append("\"").append(entry.getKey())
                    .append(onlyOnce ? "\"=coalesce(\"" + entry.getKey() + "\", ?)" : "\"=?");

            while (entries.hasNext()) {
                entry = entries.next();
                builder.append(" and ").append("\"").append(entry.getKey())
                        .append(onlyOnce ? "\"=coalesce(\"" + entry.getKey() + "\", ?)" : "\"=?");
            }
        }

        builder.append(" where " + PRIMARY_KEY + " = ?");

        try (Connection conn = queryExecutor.getConnection()) {
            PreparedStatement statement = conn.prepareStatement(builder.toString());
            int i = 1;
            for (Map.Entry<String, Object> entry : properties) {
                FieldType fieldType = columns.get(entry);
                if (fieldType == null) {
                    createColumn(project, entry.getKey(), entry.getValue());
                }
                statement.setObject(i++, getJDBCValue(fieldType, entry.getValue(), conn));
            }
            statement.setLong(i++, Long.parseLong(userId));

            i = statement.executeUpdate();
            if (i == 0) {
                createInternal(project, userId, properties);
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    private String getPostgresqlType(Class clazz) {
        if (clazz.equals(String.class)) {
            return "text";
        } else if (clazz.equals(Float.class) || clazz.equals(Double.class)) {
            return "double precision";
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
        queryExecutor.executeRawStatement(format("CREATE TABLE IF NOT EXISTS %s (" +
                "  %s TEXT NOT NULL,\n" +
                "  created_at timestamp NOT NULL,\n" +
                "  PRIMARY KEY (%s)" +
                ")", getUserTable(project), PRIMARY_KEY, PRIMARY_KEY));
    }

    @Override
    public void unsetProperties(String project, String user, List<String> properties) {
        setUserProperty(project, user, Iterables.transform(properties,
                input -> new SimpleImmutableEntry<>(input, null)), false);
    }

    @Override
    public void incrementProperty(String project, String user, String property, double value) {
        Map<String, FieldType> columns = propertyCache.getIfPresent(project);
        if (columns == null) {
            columns = getProjectProperties(project);
        }

        FieldType fieldType = columns.get(property);
        if (fieldType == null) {
            createColumn(project, property, 0);
        }

        if (fieldType != FieldType.LONG || fieldType != FieldType.DOUBLE) {
            throw new RakamException(String.format("The property the is %s and it can't be incremented.", fieldType.name()),
                    HttpResponseStatus.BAD_REQUEST);
        }

        try (Connection conn = queryExecutor.getConnection()) {
            conn.createStatement().execute("update " + getUserTable(project) + " set " + property + " += " + value);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }
}
