package org.rakam.postgresql.plugin.user;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.postgresql.util.PGobject;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.RequestContext;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.user.ISingleUserBatchOperation;
import org.rakam.plugin.user.User;
import org.rakam.plugin.user.UserStorage;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.util.DateTimeUtils;
import org.rakam.util.JsonHelper;
import org.rakam.util.ProjectCollection;
import org.rakam.util.RakamException;

import javax.annotation.Nullable;
import java.sql.*;
import java.sql.Date;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.rakam.analysis.InternalConfig.USER_TYPE;
import static org.rakam.report.QueryResult.TOTAL_RESULT;
import static org.rakam.util.JDBCUtil.fromSql;
import static org.rakam.util.ValidationUtil.*;

public abstract class AbstractPostgresqlUserStorage
        implements UserStorage {
    private final QueryExecutorService queryExecutorService;
    private final PostgresqlQueryExecutor queryExecutor;
    private final Cache<String, Map<String, FieldType>> propertyCache;
    private final LoadingCache<String, Optional<FieldType>> userTypeCache;
    private final ConfigManager configManager;
    private final ThreadPoolExecutor executor;

    public AbstractPostgresqlUserStorage(QueryExecutorService queryExecutorService, PostgresqlQueryExecutor queryExecutor, ConfigManager configManager) {
        executor = new ThreadPoolExecutor(
                0,
                Runtime.getRuntime().availableProcessors() * 4,
                60L, SECONDS,
                new SynchronousQueue<>());

        this.queryExecutorService = queryExecutorService;
        this.queryExecutor = queryExecutor;
        propertyCache = CacheBuilder.newBuilder().build();
        this.configManager = configManager;
        userTypeCache = CacheBuilder.newBuilder().build(new CacheLoader<String, Optional<FieldType>>() {
            @Override
            public Optional<FieldType> load(String key)
                    throws Exception {
                return Optional.ofNullable(configManager.getConfig(key, USER_TYPE.name(), FieldType.class));
            }
        });
    }

    public Map<String, FieldType> loadColumns(String project) {
        Map<String, FieldType> columns = getMetadata(project).stream()
                .collect(Collectors.toMap(col -> col.getName(), col -> col.getType()));
        return columns;
    }

    @Override
    public Object create(String project, Object id, ObjectNode properties) {
        try (Connection conn = queryExecutor.getConnection()) {
            return createInternal(conn, project, id, () -> properties.fields());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, FieldType> createMissingColumns(String project, Object id, Iterable<Map.Entry<String, JsonNode>> fields, Runnable schemaChangeHook) {
        Map<String, FieldType> columns = propertyCache.getIfPresent(project);
        if (columns == null) {
            columns = loadColumns(project);
            propertyCache.put(project, columns);
        }

        boolean created = false;
        for (Map.Entry<String, JsonNode> entry : fields) {
            FieldType fieldType = columns.get(entry.getKey());
            if (fieldType == null && entry.getValue() != null && !entry.getKey().equals("created_at")) {
                created = true;
                // we need it in order to prevent race condition
                if (schemaChangeHook != null) {
                    schemaChangeHook.run();
                }
                createColumn(project, id, entry.getKey(), entry.getValue());
            }
        }

        if (created) {
            columns = loadColumns(project);
            propertyCache.put(project, columns);
        }

        if (columns.isEmpty()) {
            FieldType other = (id instanceof Long ? FieldType.LONG :
                    (id instanceof Integer ? FieldType.INTEGER : FieldType.STRING));
            FieldType fieldType = configManager.setConfigOnce(project, USER_TYPE.name(), other);
            createProjectIfNotExists(project, fieldType.isNumeric());
            columns = loadColumns(project);
        }

        return columns;
    }

    public abstract QueryExecutorService getExecutorForWithEventFilter();

    private Iterable<Map.Entry<String, JsonNode>> strip(Iterable<Map.Entry<String, JsonNode>> fields) {
        ObjectNode properties = JsonHelper.jsonObject();

        for (Map.Entry<String, JsonNode> entry : fields) {
            String key = stripName(entry.getKey(), "property");
            if (key.equals("id")) {
                key = "_id";
            }

            properties.set(key, entry.getValue());
        }

        return () -> properties.fields();
    }

    public Object createInternal(Connection conn, String project, Object id, Iterable<Map.Entry<String, JsonNode>> _properties)
            throws SQLException {
        Iterable<Map.Entry<String, JsonNode>> properties = strip(_properties);

        Map<String, FieldType> columns = createMissingColumns(project, id, properties, new CommitConnection(conn));

        StringBuilder cols = new StringBuilder();
        StringBuilder parametrizedValues = new StringBuilder();
        Iterator<Map.Entry<String, JsonNode>> stringIterator = properties.iterator();

        if (stringIterator.hasNext()) {
            while (stringIterator.hasNext()) {
                Map.Entry<String, JsonNode> next = stringIterator.next();

                if (!next.getKey().equals(PRIMARY_KEY) && !next.getKey().equals("created_at")) {
                    if (!columns.containsKey(next.getKey())) {
                        continue;
                    }
                    if (cols.length() > 0) {
                        cols.append(", ");
                        parametrizedValues.append(", ");
                    }
                    cols.append(checkTableColumn(next.getKey()));
                    parametrizedValues.append('?');
                }
            }
        }

        if (parametrizedValues.length() > 0) {
            parametrizedValues.append(", ");
        }
        parametrizedValues.append('?');

        if (cols.length() > 0) {
            cols.append(", ");
        }
        cols.append("created_at");

        if (id != null) {
            parametrizedValues.append(", ").append('?');
            cols.append(", ").append(PRIMARY_KEY);
        }

        ProjectCollection userTable = getUserTable(project, false);

        String table = checkProject(userTable.project, '"') + "." + checkCollection(userTable.collection);

        PreparedStatement statement = conn.prepareStatement(
                "INSERT INTO  " + table + " (" + cols + ") " +
                        "values (" + parametrizedValues + ") RETURNING " + PRIMARY_KEY);

        long createdAt = -1;
        int i = 1;
        for (Map.Entry<String, JsonNode> o : properties) {
            if (o.getKey().equals(PRIMARY_KEY)) {
                throw new RakamException(String.format("User property %s is invalid. It's used as primary key", PRIMARY_KEY), BAD_REQUEST);
            }

            if (!columns.containsKey(o.getKey())) {
                continue;
            }

            if (o.getKey().equals("created_at")) {
                try {
                    createdAt = DateTimeUtils.parseTimestamp(o.getValue().isNumber() ? o.getValue().numberValue()
                            : o.getValue().textValue());
                } catch (Exception e) {
                    createdAt = Instant.now().toEpochMilli();
                }
            } else {
                FieldType fieldType = columns.get(o.getKey());
                statement.setObject(i++, getJDBCValue(fieldType, o.getValue(), conn));
            }
        }

        statement.setTimestamp(i++, new java.sql.Timestamp(createdAt == -1 ? Instant.now().toEpochMilli() : createdAt));
        if (id != null) {
            statement.setObject(i++, id);
        }

        ResultSet resultSet;
        try {
            resultSet = statement.executeQuery();
        } catch (SQLException e) {
            if (e.getMessage().contains("duplicate key value")) {
                setUserProperties(conn, project, id, properties, false);
                return id;
            } else {
                throw e;
            }
        }
        resultSet.next();
        Object object = resultSet.getObject(1);
        resultSet.close();
        return object;
    }

    private String sqlArrayTypeName(FieldType fieldType) {
        if (fieldType.isArray()) {
            throw new UnsupportedOperationException();
        }
        switch (fieldType) {
            case BOOLEAN:
                return "bool";
            case STRING:
                return "varchar";
            case DOUBLE:
                return "float8";
            case LONG:
                return "int8";
            case INTEGER:
                return "int4";
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

    public Object getJDBCValue(FieldType fieldType, JsonNode value, Connection conn)
            throws SQLException {
        if (value == NullNode.getInstance() || value == null) {
            return null;
        }
        if (fieldType.isArray()) {
            if (value.isArray()) {
                FieldType arrayType = fieldType.getArrayElementType();

                Object[] objects = new Object[value.size()];
                for (int i = 0; i < value.size(); i++) {
                    objects[i] = getJDBCValue(arrayType, value.get(i), conn);
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
                try {
                    return new Timestamp(value.isNumber() ? DateTimeUtils.parseTimestamp(value.asLong())
                            : DateTimeUtils.parseTimestamp(value.textValue()));
                } catch (Exception e) {
                    return null;
                }
            case DATE:
                try {
                    return new Timestamp(DateTimeUtils.parseDate(value.textValue()));
                } catch (Exception e) {
                    return null;
                }
            case LONG:
                return value.asLong();
            case DECIMAL:
            case DOUBLE:
                return value.asDouble();
            case INTEGER:
                return value.asInt();
            case STRING:
                return value.asText();
            case TIME:
                return parseTime(value);
            case BOOLEAN:
                return value.asBoolean();
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

    @Override
    public List<Object> batchCreate(RequestContext context, List<User> users) {
        // may use transaction when we start to use Postgresql 9.5. Since we use insert or merge, it doesn't work right now.
        return users.stream()
                .map(user -> {
                    Object o = create(context.project, user.id, user.properties);
                    if (user.api != null) {
                        throw new RakamException("api property in User object is not allowed in batch endpoint", BAD_REQUEST);
                    }
                    return o;
                })
                .collect(Collectors.toList());
    }

    private void createColumn(String project, Object id, String column, JsonNode value) {
        createColumnInternal(project, id, column, value, true);
    }

    private void createColumnInternal(String project, Object id, String column, JsonNode value, boolean retry) {
        // it must be called from a separated transaction, otherwise it may lock table and the other insert may cause deadlock.
        try (Connection conn = queryExecutor.getConnection()) {
            try {
                if (value.equals(NullNode.getInstance())) {
                    return;
                }
                ProjectCollection userTable = getUserTable(project, false);
                conn.createStatement().execute(format("alter table %s.%s add column %s %s",
                        checkProject(userTable.project, '"'),
                        checkCollection(userTable.collection),
                        checkTableColumn(column), getPostgresqlType(value)));
            } catch (SQLException e) {
                Map<String, FieldType> fields = loadColumns(project);
                if (fields.containsKey(column)) {
                    return;
                }

                if (getMetadata(new RequestContext(project, null)).stream()
                        .anyMatch(col -> col.getName().equals(column))) {
                    // what if the type does not match?
                    return;
                }

                if (retry) {
                    FieldType other = (id instanceof Long ? FieldType.LONG :
                            (id instanceof Integer ? FieldType.INTEGER : FieldType.STRING));
                    FieldType fieldType = configManager.setConfigOnce(project, USER_TYPE.name(), other);

                    createProjectIfNotExists(project, fieldType.isNumeric());

                    createColumnInternal(project, id, column, value, false);
                } else {
                    throw e;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract List<String> getEventFilterPredicate(String project, List<EventFilter> eventFilter);

    @Override
    public CompletableFuture<QueryResult> searchUsers(RequestContext context, List<String> selectColumns, Expression filterExpression, List<EventFilter> eventFilter, Sorting sortColumn, long limit, String offset) {
        checkProject(context.project);
        List<SchemaField> metadata = getMetadata(context);

        if (metadata.isEmpty()) {
            return CompletableFuture.completedFuture(QueryResult.empty());
        }
        Stream<SchemaField> projectColumns = metadata.stream();
        if (selectColumns != null) {
            projectColumns = projectColumns.filter(column -> selectColumns.contains(column.getName()));
        }
        // TODO: fail id column is not exist.
        String columns = Joiner.on(", ").join(projectColumns.map(col -> checkTableColumn(col.getName()))
                .toArray());

        LinkedList<String> filters = new LinkedList<>();
        if (filterExpression != null) {
            filters.add(new ExpressionFormatter.Formatter(Optional.empty()).process(filterExpression, null));
        }

        if (eventFilter != null && !eventFilter.isEmpty()) {
            filters.addAll(getEventFilterPredicate(context.project, eventFilter));
        }

        if (sortColumn != null && !metadata.stream().anyMatch(col -> col.getName().equals(sortColumn.column))) {
            throw new IllegalArgumentException(format("sorting column does not exist: %s", sortColumn.column));
        }

        String orderBy = sortColumn == null ? "" : format(" ORDER BY %s %s", sortColumn.column, sortColumn.order);

        boolean isEventFilterActive = eventFilter != null && !eventFilter.isEmpty();

        QueryExecutorService service = isEventFilterActive ? getExecutorForWithEventFilter() : this.queryExecutorService;
        QueryExecution query = service.executeQuery(context, format("SELECT %s FROM _users %s %s LIMIT %s",
                columns, filters.isEmpty() ? "" : " WHERE "
                        + Joiner.on(" AND ").join(filters), orderBy, limit, offset), ZoneOffset.UTC);

        CompletableFuture<QueryResult> dataResult = query.getResult();

        if (!isEventFilterActive) {
            StringBuilder builder = new StringBuilder();
            builder.append("SELECT count(*) FROM _users");
            if (filterExpression != null) {
                builder.append(" WHERE ").append(filters.get(0));
            }

            QueryExecution totalResult = queryExecutorService.executeQuery(context, builder.toString(), ZoneOffset.UTC);

            CompletableFuture<QueryResult> result = new CompletableFuture<>();
            CompletableFuture.allOf(dataResult, totalResult.getResult()).whenComplete((__, ex) -> {
                QueryResult data = dataResult.join();
                QueryResult totalResultData = totalResult.getResult().join();
                if (ex == null && !data.isFailed() && !totalResultData.isFailed()) {
                    Object v1 = totalResultData.getResult().get(0).get(0);
                    result.complete(new QueryResult(data.getMetadata(), data.getResult(),
                            ImmutableMap.of(TOTAL_RESULT, v1)));
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
    public List<SchemaField> getMetadata(RequestContext context) {
        return getMetadata(context.project);
    }

    public List<SchemaField> getMetadata(String project) {
        checkProject(project);
        LinkedList<SchemaField> columns = new LinkedList<>();

        try (Connection conn = queryExecutor.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            ProjectCollection userTable = getUserTable(project, false);

            ResultSet indexInfo = metaData.getIndexInfo(null, userTable.project, userTable.collection, true, false);
            ResultSet dbColumns = metaData.getColumns(null, userTable.project, userTable.collection, null);

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
                columns.add(new SchemaField(columnName, fieldType, null, null, null));
            }
            return columns;
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't get metadata from plugin.user.storage", e);
        }
    }

    public abstract ProjectCollection getUserTable(String project, boolean isEventFilterActive);

    @Override
    public CompletableFuture<User> getUser(RequestContext context, Object userId) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = queryExecutor.getConnection()) {
                ProjectCollection userTable = getUserTable(context.project, false);
                String table = checkProject(userTable.project, '"') + "." + checkCollection(userTable.collection);

                PreparedStatement ps = conn.prepareStatement(format("select * from %s where %s = ?", table, PRIMARY_KEY));

                Optional<FieldType> unchecked = userTypeCache.getUnchecked(context.project);

                if (!unchecked.isPresent() || !unchecked.get().isNumeric()) {
                    ps.setString(1, userId.toString());
                } else if (unchecked.get() == FieldType.LONG) {
                    long x;
                    try {
                        x = Long.parseLong(userId.toString());
                    } catch (NumberFormatException e) {
                        throw new RakamException("User id is invalid", BAD_REQUEST);
                    }

                    ps.setLong(1, x);
                } else if (unchecked.get() == FieldType.INTEGER) {
                    int x;
                    try {
                        x = Integer.parseInt(userId.toString());
                    } catch (NumberFormatException e) {
                        throw new RakamException("User id is invalid", BAD_REQUEST);
                    }

                    ps.setInt(1, x);
                }

                ResultSet resultSet = ps.executeQuery();

                ObjectNode properties = JsonHelper.jsonObject();
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount() + 1;

                while (resultSet.next()) {
                    for (int i = 1; i < columnCount; i++) {
                        String key = metaData.getColumnName(i);
                        if (!key.equals(PRIMARY_KEY)) {
                            FieldType fieldType = fromSql(metaData.getColumnType(i), metaData.getColumnTypeName(i));
                            JsonNode value = setValues(resultSet, i, fieldType);
                            if (!value.equals(NullNode.getInstance())) {
                                properties.set(key, value);
                            }
                        }
                    }
                }
                return new User(userId, null, properties);
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }
        });
    }

    private JsonNode setValues(ResultSet resultSet, int i, FieldType fieldType)
            throws SQLException {
        JsonNode node;
        switch (fieldType) {
            case STRING:
                String string = resultSet.getString(i);
                if (resultSet.wasNull()) return NullNode.getInstance();
                node = JsonHelper.textNode(string);
                break;
            case INTEGER:
                int anInt = resultSet.getInt(i);
                if (resultSet.wasNull()) return NullNode.getInstance();
                node = JsonHelper.numberNode(anInt);
                break;
            case LONG:
                long aLong = resultSet.getLong(i);
                if (resultSet.wasNull()) return NullNode.getInstance();
                node = JsonHelper.numberNode(aLong);
                break;
            case BOOLEAN:
                boolean aBoolean = resultSet.getBoolean(i);
                if (resultSet.wasNull()) return NullNode.getInstance();
                node = JsonHelper.booleanNode(aBoolean);
                break;
            case DATE:
                Date date = resultSet.getDate(i);
                if (resultSet.wasNull()) return NullNode.getInstance();
                node = JsonHelper.textNode(date.toLocalDate().toString());
                break;
            case TIMESTAMP:
                Timestamp timestamp = resultSet.getTimestamp(i);
                if (resultSet.wasNull()) return NullNode.getInstance();
                node = JsonHelper.textNode(timestamp.toInstant().toString());
                break;
            case TIME:
                Time time = resultSet.getTime(i);
                if (resultSet.wasNull()) return NullNode.getInstance();
                node = JsonHelper.textNode(time.toInstant().toString());
                break;
            case BINARY:
                byte[] bytes = resultSet.getBytes(i);
                if (resultSet.wasNull()) return NullNode.getInstance();
                node = JsonHelper.binaryNode(bytes);
                break;
            case DOUBLE:
            case DECIMAL:
                double aDouble = resultSet.getDouble(i);
                if (resultSet.wasNull()) return NullNode.getInstance();
                node = JsonHelper.numberNode(aDouble);
                break;
            default:
                if (fieldType.isArray()) {
                    ArrayNode jsonNodes = JsonHelper.jsonArray();
                    Array array = resultSet.getArray(i);
                    if (resultSet.wasNull()) return NullNode.getInstance();

                    ResultSet rs = array.getResultSet();
                    int arrIdx = 1;
                    if (rs.next()) {
                        jsonNodes.add(setValues(rs, arrIdx++, fieldType.getArrayElementType()));
                    }

                    node = jsonNodes;
                } else if (fieldType.isMap()) {
                    PGobject pgObject = (PGobject) resultSet.getObject(i + 1);
                    if (resultSet.wasNull()) return NullNode.getInstance();

                    node = JsonHelper.read(pgObject.getValue());
                } else {
                    throw new UnsupportedOperationException();
                }
        }

        return node;
    }

    @Override
    public void setUserProperties(String project, Object userId, ObjectNode properties) {
        try (Connection conn = queryExecutor.getConnection()) {
            setUserProperties(conn, project, userId, () -> properties.fields(), false);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void setUserPropertiesOnce(String project, Object userId, ObjectNode properties) {
        try (Connection conn = queryExecutor.getConnection()) {
            setUserProperties(conn, project, userId, () -> properties.fields(), true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void setUserProperties(Connection connection, String project, Object userId, Iterable<Map.Entry<String, JsonNode>> _properties, boolean onlyOnce)
            throws SQLException {
        if (userId == null) {
            throw new RakamException("User id is not set.", BAD_REQUEST);
        }

        Iterable<Map.Entry<String, JsonNode>> properties = strip(_properties);
        if (!properties.iterator().hasNext()) {
            return;
        }

        Map<String, FieldType> columns = createMissingColumns(project, userId, properties, new CommitConnection(connection));

        ProjectCollection userTable = getUserTable(project, false);
        String table = checkProject(userTable.project, '"') + "." + checkCollection(userTable.collection);

        StringBuilder builder = new StringBuilder("update " + table + " set ");
        Iterator<Map.Entry<String, JsonNode>> entries = properties.iterator();
        boolean hasColumn = false;
        while (entries.hasNext()) {
            Map.Entry<String, JsonNode> entry = entries.next();

            if (!columns.containsKey(entry.getKey())) {
                continue;
            }

            if (!hasColumn) {
                hasColumn = true;
            } else {
                builder.append(", ");
            }

            builder.append(checkTableColumn(entry.getKey()))
                    .append((onlyOnce || entry.getKey().equals("created_at")) ?
                            " = coalesce(" + checkTableColumn(entry.getKey()) + ", ?)" : " = ?");
        }

        if (!hasColumn) {
            builder.append("created_at = created_at");
        }

        builder.append(" where " + PRIMARY_KEY + " = ?");

        PreparedStatement statement = connection.prepareStatement(builder.toString());
        int i = 1;
        Iterator<Map.Entry<String, JsonNode>> fields = properties.iterator();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            FieldType fieldType = columns.get(entry.getKey());
            if (fieldType == null) {
                continue;
            }
            statement.setObject(i++, getJDBCValue(fieldType, entry.getValue(), connection));
        }

        setUserId(project, statement, userId, i++);

        i = statement.executeUpdate();
        statement.close();

        if (i == 0) {
            createInternal(connection, project, userId, properties);
        }
    }

    public void setUserId(String project, PreparedStatement statement, Object userId, int position)
            throws SQLException {
        Optional<FieldType> fieldType = userTypeCache.getUnchecked(project);
        if (!fieldType.isPresent() || fieldType.get() == FieldType.STRING) {
            statement.setString(position, userId.toString());
        } else if (fieldType.get() == FieldType.INTEGER) {
            statement.setInt(position, (userId instanceof Number) ? ((Number) userId).intValue() :
                    Integer.parseInt(userId.toString()));
        } else if (fieldType.get() == FieldType.LONG) {
            statement.setLong(position, (userId instanceof Number) ? ((Number) userId).longValue() :
                    Integer.parseInt(userId.toString()));
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public void createProjectIfNotExists(String project, boolean userIdIsNumeric) {
        ProjectCollection userTable = getUserTable(project, false);
        String table = checkProject(userTable.project, '"') + "." + checkCollection(userTable.collection);

        QueryResult join = queryExecutor.executeRawStatement(new RequestContext(project, null), format("CREATE TABLE IF NOT EXISTS %s (" +
                "  %s " + (userIdIsNumeric ? "serial" : "text") + " NOT NULL,\n" +
                "  created_at timestamp NOT NULL,\n" +
                "  PRIMARY KEY (%s)" +
                ")", table, PRIMARY_KEY, PRIMARY_KEY)).getResult().join();
        if (join.isFailed()) {
            throw new RakamException(join.getError().toString(), INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public void dropProjectIfExists(String project) {
        ProjectCollection userTable = getUserTable(project, false);

        String table = checkProject(userTable.project, '"') + "." + checkCollection(userTable.collection);

        QueryResult result = queryExecutor.executeRawStatement(new RequestContext(project, null), format("DROP TABLE IF EXISTS %s",
                table)).getResult().join();
        propertyCache.invalidateAll();
        userTypeCache.invalidateAll();
        if (result.isFailed()) {
            throw new IllegalStateException(result.toString());
        }
    }

    @Override
    public void unsetProperties(String project, Object user, List<String> properties) {
        try (Connection conn = queryExecutor.getConnection()) {
            unsetProperties(conn, project, user, properties);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void incrementProperty(String project, Object userId, String property, double value) {
        try (Connection conn = queryExecutor.getConnection()) {
            incrementProperty(conn, project, userId, property, value);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public void unsetProperties(Connection connection, String project, Object user, List<String> properties)
            throws SQLException {
        setUserProperties(connection, project, user, Iterables.transform(properties, new com.google.common.base.Function<String, Map.Entry<String, JsonNode>>() {
            @Nullable
            public Map.Entry<String, JsonNode> apply(@Nullable String input) {
                return new SimpleImmutableEntry<>(input, NullNode.getInstance());
            }
        }), false);
    }

    public void incrementProperty(Connection conn, String project, Object userId, String property, double value)
            throws SQLException {
        Map<String, FieldType> columns = createMissingColumns(project, userId, ImmutableList.of(new SimpleImmutableEntry<>(property, new DoubleNode(value))), new CommitConnection(conn));

        FieldType fieldType = columns.get(property);
        if (fieldType == null) {
            createColumn(project, userId, property, JsonHelper.numberNode(0));
        }

        if (!fieldType.isNumeric()) {
            throw new RakamException(String.format("The property the is %s and it can't be incremented.", fieldType.name()),
                    BAD_REQUEST);
        }

        String tableRef = checkTableColumn(stripName(property, "table column"));
        Statement statement = conn.createStatement();
        ProjectCollection userTable = getUserTable(project, false);

        String table = checkProject(userTable.project, '"') + "." + checkCollection(userTable.collection);

        int execute = statement.executeUpdate("update " + table +
                " set " + tableRef + " = " + value + " + coalesce(" + tableRef + ", 0)");
        if (execute == 0) {
            create(project, userId, JsonHelper.jsonObject().put(property, value));
        }
    }

    @Override
    public CompletableFuture<Void> batch(String project, List<? extends ISingleUserBatchOperation> operations) {
        try {
            return CompletableFuture.runAsync(() -> {
                try (Connection conn = queryExecutor.getConnection()) {
                    conn.setAutoCommit(false);

                    int i = 1;
                    for (ISingleUserBatchOperation operation : operations) {
                        if (operation.getSetProperties() != null) {
                            setUserProperties(conn, project, operation.getUser(), () -> operation.getSetProperties().fields(), false);
                        }
                        if (operation.getSetPropertiesOnce() != null) {
                            setUserProperties(conn, project, operation.getUser(), () -> operation.getSetPropertiesOnce().fields(), true);
                        }
                        if (operation.getUnsetProperties() != null) {
                            unsetProperties(conn, project, operation.getUser(), operation.getUnsetProperties());
                        }
                        if (operation.getIncrementProperties() != null) {
                            for (Map.Entry<String, Double> entry : operation.getIncrementProperties().entrySet()) {
                                incrementProperty(conn, project, operation.getUser(), entry.getKey(), entry.getValue());
                            }
                        }

                        if (i++ % 5000 == 0) {
                            conn.commit();
                        }
                    }

                    conn.commit();
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } catch (Exception e) {
                    throw e;
                }
            }, executor);
        } catch (RejectedExecutionException e) {
            throw new RakamException("Please slow down", BAD_REQUEST);
        }
    }

    private String getPostgresqlType(JsonNode clazz) {
        if (clazz.isTextual()) {
            try {
                DateTimeUtils.parseDate(clazz.asText());
                return "date";
            } catch (Exception e) {

            }

            try {
                DateTimeUtils.parseTimestamp(clazz.asText());
                return "timestamp";
            } catch (Exception e) {

            }

            return "text";
        } else if (clazz.isFloat() || clazz.isDouble()) {
            return "float8";
        } else if (clazz.isNumber()) {
            return "int8";
        } else if (clazz.isBoolean()) {
            return "bool";
        } else if (clazz.isArray()) {
            if(clazz.get(0).isObject()){
                return "jsonb";
            }
            return getPostgresqlType(clazz.get(0)) + "[]";
        } else if (clazz.isObject()) {
            return "jsonb";
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static class CommitConnection
            implements Runnable {

        private final Connection connection;

        public CommitConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void run() {
            try {
                if (!connection.getAutoCommit()) {
                    connection.commit();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
