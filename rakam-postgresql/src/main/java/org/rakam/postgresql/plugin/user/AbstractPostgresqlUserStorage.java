package org.rakam.postgresql.plugin.user;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.postgresql.util.PGobject;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.InternalConfig;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.user.User;
import org.rakam.plugin.user.UserStorage;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.rakam.postgresql.analysis.PostgresqlMetastore.fromSql;
import static org.rakam.util.ValidationUtil.checkProject;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public abstract class AbstractPostgresqlUserStorage implements UserStorage {
    private final PostgresqlQueryExecutor queryExecutor;
    private final Cache<String, Map<String, FieldType>> propertyCache;
    private final LoadingCache<String, Optional<FieldType>> userTypeCache;
    private final ConfigManager configManager;

    public AbstractPostgresqlUserStorage(PostgresqlQueryExecutor queryExecutor, ConfigManager configManager) {
        this.queryExecutor = queryExecutor;
        propertyCache = CacheBuilder.newBuilder().build();
        this.configManager = configManager;
        userTypeCache = CacheBuilder.newBuilder().build(new CacheLoader<String, Optional<FieldType>>() {
            @Override
            public Optional<FieldType> load(String key) throws Exception {
                FieldType config = configManager.getConfig(key, InternalConfig.USER_TYPE.name(), FieldType.class);
                return config == null ? Optional.<FieldType>empty() : Optional.of(config);
            }
        });
    }

    public Map<String, FieldType> loadColumns(String project) {
        Map<String, FieldType> columns = getMetadata(project).stream()
                .collect(Collectors.toMap(col -> col.getName(), col -> col.getType()));
        return columns;
    }

    @Override
    public Object create(String project, Object id, Map<String, Object> properties) {
        return createInternal(project, id, properties.entrySet());
    }

    private Map<String, FieldType> createMissingColumns(String project, Iterable<Map.Entry<String, Object>> properties) {
        Map<String, FieldType> columns = propertyCache.getIfPresent(project);
        if (columns == null) {
            columns = loadColumns(project);
            propertyCache.put(project, columns);
        }

        boolean created = false;
        for (Map.Entry<String, Object> entry : properties) {
            FieldType fieldType = columns.get(entry.getKey());
            if (fieldType == null && entry.getValue() != null) {
                created = true;
                createColumn(project, entry.getKey(), entry.getValue());
            }
        }

        if(created) {
            columns = loadColumns(project);
            propertyCache.put(project, columns);
        }

        return columns;
    }

    public abstract QueryExecutor getExecutorForWithEventFilter();

    public Object createInternal(String project, Object id,
                                 Iterable<Map.Entry<String, Object>> _properties) {
        List<Map.Entry<String, Object>> properties = new ArrayList<>();

        for (Map.Entry<String, Object> entry : _properties) {
            String key = checkTableColumn(entry.getKey());
            if(!key.equals(entry.getKey())) {
                properties.add(new SimpleImmutableEntry<>(key, entry.getValue()));
            } else {
                properties.add(entry);
            }
        }

        Map<String, FieldType> columns = createMissingColumns(project, properties);

        try (Connection conn = queryExecutor.getConnection()) {
            if (id == null) {
                FieldType unchecked = userTypeCache.getUnchecked(project).orElse(null);
                if (unchecked == null) {
                    unchecked = configManager.setConfigOnce(project,
                            InternalConfig.USER_TYPE.name(), FieldType.STRING);

                }

                if (unchecked.isNumeric()) {
                    id = new Random().nextInt();
                } else {
                    id = UUID.randomUUID().toString();
                }
            }

            StringBuilder cols = new StringBuilder();
            StringBuilder parametrizedValues = new StringBuilder();
            Iterator<Map.Entry<String, Object>> stringIterator = properties.iterator();

            if (stringIterator.hasNext()) {
                Map.Entry<String, Object> next = stringIterator.next();

                if (!next.getKey().equals(PRIMARY_KEY) && !next.getKey().equals("created_at")) {
                    parametrizedValues.append('?');
                    cols.append(next.getKey());
                }

                while (stringIterator.hasNext()) {
                    next = stringIterator.next();

                    if (!next.getKey().equals(PRIMARY_KEY) && !next.getKey().equals("created_at")) {
                        cols.append(", ").append(next.getKey());
                        parametrizedValues.append(", ").append('?');
                    }
                }
            }

            parametrizedValues.append(", ").append('?');
            cols.append(", ").append("created_at");

            if (id != null) {
                parametrizedValues.append(", ").append('?');
                cols.append(", ").append(PRIMARY_KEY);
            }

            PreparedStatement statement = conn.prepareStatement("INSERT INTO  " + getUserTable(project, false) + " (" + cols +
                    ") values (" + parametrizedValues + ") RETURNING " + PRIMARY_KEY);

            Instant createdAt = null;
            int i = 1;
            for (Map.Entry<String, Object> o : properties) {
                if (o.getKey().equals(PRIMARY_KEY)) {
                    throw new RakamException(String.format("User property %s is invalid. It's used as primary key", PRIMARY_KEY), HttpResponseStatus.BAD_REQUEST);
                }

                if (o.getKey().equals("created_at")) {
                    if (o.getValue() instanceof String) {
                        createdAt = Instant.parse(o.getValue().toString());
                    }
                } else {
                    FieldType fieldType = columns.get(o.getKey());
                    statement.setObject(i++, getJDBCValue(fieldType, o.getValue(), conn));
                }
            }

            statement.setTimestamp(i++, java.sql.Timestamp.from(createdAt == null ? Instant.now() : createdAt));
            if (id != null) {
                statement.setObject(i++, id);
            }

            ResultSet resultSet;
            try {
                resultSet = statement.executeQuery();
            } catch (SQLException e) {
                if (e.getMessage().contains("duplicate key value")) {
                    setUserProperty(project, id, properties, false);
                    return id;
                } else {
                    throw e;
                }
            }
            resultSet.next();
            return resultSet.getObject(1);
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
            case INTEGER:
                return "int";
            case DECIMAL:
                return "decimal";
            case TIMESTAMP:
                return "timestamp";
            case TIME:
                return "time";
            case DATE:
                return "date";
            default:
                if (fieldType.isArray()) {
                    return fieldType.getArrayElementType() + "[]";
                }
                if (fieldType.isMap()) {
                    return "jsonb";
                }
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
            case INTEGER:
            case DECIMAL:
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
    public List<Object> batchCreate(String project, List<User> users) {
        // may use transaction when we start to use Postgresql 9.5. Since we use insert or merge, it doesn't work right now.
        return users.stream()
                .map(user -> create(project, user.id, user.properties))
                .collect(Collectors.toList());
    }

    private void createColumn(String project, String column, Object value) {
        createColumnInternal(project, column, value, true);
    }

    private void createColumnInternal(String project, String column, Object value, boolean retry) {
        // it must be called from a separated transaction, otherwise it may lock table and the other insert may cause deadlock.
        try (Connection conn = queryExecutor.getConnection()) {
            try {
                conn.createStatement().execute(format("alter table %s add column %s %s",
                        getUserTable(project, false), column, getPostgresqlType(value.getClass())));
            } catch (SQLException e) {
                Map<String, FieldType> fields = loadColumns(project);
                if (fields.containsKey(column)) {
                    return;
                }

                if (retry) {
                    userTypeCache.refresh(project);
                    createProjectIfNotExists(project,
                            userTypeCache.getUnchecked(project).orElse(FieldType.STRING).isNumeric());
                    createColumnInternal(project, column, value, false);
                    return;
                }

                throw e;
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

        if (metadata.isEmpty()) {
            return CompletableFuture.completedFuture(QueryResult.empty());
        }
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

        if (sortColumn != null && !metadata.stream().anyMatch(col -> col.getName().equals(sortColumn.column))) {
            throw new IllegalArgumentException(format("sorting column does not exist: %s", sortColumn.column));
        }

        String orderBy = sortColumn == null ? "" : format(" ORDER BY %s %s", sortColumn.column, sortColumn.order);

        boolean isEventFilterActive = eventFilter != null && !eventFilter.isEmpty();

        QueryExecution query = (isEventFilterActive ? getExecutorForWithEventFilter() : queryExecutor)
                .executeRawQuery(format("SELECT %s FROM %s %s %s LIMIT %s",
                        columns, getUserTable(project, isEventFilterActive), filters.isEmpty() ? "" : " WHERE " + Joiner.on(" AND ").join(filters), orderBy, limit, offset));

        CompletableFuture<QueryResult> dataResult = query.getResult();

        if (!isEventFilterActive) {
            StringBuilder builder = new StringBuilder();
            builder.append("SELECT count(*) FROM " + getUserTable(project, false));
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
            String[] userTable = getUserTable(project, false).split("\\.", 2);
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

    public abstract String getUserTable(String project, boolean isEventFilterActive);

    @Override
    public CompletableFuture<User> getUser(String project, Object userId) {
        checkProject(project);
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = queryExecutor.getConnection()) {
                PreparedStatement ps = conn.prepareStatement(format("select * from %s where %s = ?", getUserTable(project, false), PRIMARY_KEY));

                Optional<FieldType> unchecked = userTypeCache.getUnchecked(project);

                if (!unchecked.isPresent()) {
                    return null;
                }

                if (!unchecked.get().isNumeric()) {
                    ps.setString(1, userId.toString());
                } else if (unchecked.get() == FieldType.LONG) {
                    long x;
                    try {
                        x = Long.parseLong(userId.toString());
                    } catch (NumberFormatException e) {
                        throw new RakamException("User id is invalid", HttpResponseStatus.BAD_REQUEST);
                    }

                    ps.setLong(1, x);
                } else if (unchecked.get() == FieldType.INTEGER) {
                    int x;
                    try {
                        x = Integer.parseInt(userId.toString());
                    } catch (NumberFormatException e) {
                        throw new RakamException("User id is invalid", HttpResponseStatus.BAD_REQUEST);
                    }

                    ps.setInt(1, x);
                }

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
                return new User(id, null, properties);
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }
        });
    }

    @Override
    public void setUserProperty(String project, Object userId, Map<String, Object> properties) {
        setUserProperty(project, userId, properties.entrySet(), false);
    }

    @Override
    public void setUserPropertyOnce(String project, Object userId, Map<String, Object> properties) {
        setUserProperty(project, userId, properties.entrySet(), true);
    }

    public void setUserProperty(String project, Object userId, Iterable<Map.Entry<String, Object>> _properties, boolean onlyOnce) {
        if(userId == null) {
            throw new RakamException("User id is not set.", HttpResponseStatus.BAD_REQUEST);
        }

        List<Map.Entry<String, Object>> properties = new ArrayList<>();

        for (Map.Entry<String, Object> entry : _properties) {
            String key = checkTableColumn(entry.getKey());
            if(!key.equals(entry.getKey())) {
                properties.add(new SimpleImmutableEntry<>(key, entry.getValue()));
            } else {
                properties.add(entry);
            }
        }

        Map<String, FieldType> columns = createMissingColumns(project, properties);

        StringBuilder builder = new StringBuilder("update " + getUserTable(project, false) + " set ");
        Iterator<Map.Entry<String, Object>> entries = properties.iterator();
        if (entries.hasNext()) {
            Map.Entry<String, Object> entry = entries.next();
            builder.append('"').append(entry.getKey())
                    .append(onlyOnce ? "\"= coalesce(\"" + entry.getKey() + "\", ?)" : "\"=?");

            while (entries.hasNext()) {
                entry = entries.next();
                builder.append(" and ").append('"').append(entry.getKey())
                        .append(onlyOnce ? "\"= coalesce(\"" + entry.getKey() + "\", ?)" : "\"=?");
            }
        }

        builder.append(" where " + PRIMARY_KEY + " = ?");

        try (Connection conn = queryExecutor.getConnection()) {
            PreparedStatement statement = conn.prepareStatement(builder.toString());
            int i = 1;
            for (Map.Entry<String, Object> entry : properties) {
                FieldType fieldType = columns.get(entry.getKey());
                if (fieldType == null) {
                    createColumn(project, entry.getKey(), entry.getValue());
                }
                statement.setObject(i++, getJDBCValue(fieldType, entry.getValue(), conn));
            }
            FieldType fieldType = userTypeCache.getUnchecked(project).orElse(FieldType.STRING);
            if(fieldType == FieldType.STRING) {
                statement.setString(i++, userId.toString());
            } else
            if(fieldType == FieldType.INTEGER) {
                statement.setInt(i++, (userId instanceof Number) ? ((Number) userId).intValue() :
                        Integer.parseInt(userId.toString()));
            } else
            if(fieldType == FieldType.LONG) {
                statement.setLong(i++, (userId instanceof Number) ? ((Number) userId).longValue() :
                        Integer.parseInt(userId.toString()));
            } else {
                throw new IllegalStateException();
            }

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
        } else if (Map.class.isAssignableFrom(clazz)) {
            return "jsonb";
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void createProjectIfNotExists(String project, boolean userIdIsNumeric) {
        checkProject(project);
        queryExecutor.executeRawStatement(format("CREATE TABLE IF NOT EXISTS %s (" +
                "  %s " + (userIdIsNumeric ? "serial" : "text") + " NOT NULL,\n" +
                "  created_at timestamp NOT NULL,\n" +
                "  PRIMARY KEY (%s)" +
                ")", getUserTable(project, false), PRIMARY_KEY, PRIMARY_KEY)).getResult().join();
    }

    @Override
    public void unsetProperties(String project, Object user, List<String> properties) {
        setUserProperty(project, user, Iterables.transform(properties,
                input -> new SimpleImmutableEntry<>(input, null)), false);
    }

    @Override
    public void incrementProperty(String project, Object user, String property, double value) {
        Map<String, FieldType> columns = createMissingColumns(project, ImmutableList.of(new SimpleImmutableEntry<>(property, 1L)));

        FieldType fieldType = columns.get(property);
        if (fieldType == null) {
            createColumn(project, property, 0);
        }

        if (!fieldType.isNumeric()) {
            throw new RakamException(String.format("The property the is %s and it can't be incremented.", fieldType.name()),
                    HttpResponseStatus.BAD_REQUEST);
        }

        try (Connection conn = queryExecutor.getConnection()) {
            conn.createStatement().execute("update " + getUserTable(project, false) + " set " + property + " += " + value);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }
}
