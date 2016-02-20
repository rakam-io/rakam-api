package org.rakam.presto.analysis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.AbstractMetastore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.util.CryptUtil;
import org.rakam.util.NotExistsException;
import org.rakam.util.ProjectCollection;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.ConnectionFactory;
import org.skife.jdbi.v2.util.IntegerMapper;
import org.skife.jdbi.v2.util.StringMapper;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.rakam.postgresql.analysis.PostgresqlMetastore.fromSql;
import static org.rakam.util.ValidationUtil.checkProject;

@Singleton
public class JDBCMetastore extends AbstractMetastore {
    private final DBI dbi;
    private final LoadingCache<ProjectCollection, List<SchemaField>> schemaCache;
    private final LoadingCache<String, Set<String>> collectionCache;
    private final LoadingCache<String, List<Set<String>>> apiKeyCache;
    private final ConnectionFactory prestoConnectionFactory;
    private final PrestoConfig config;
    private static final Map<String, FieldType> REVERSE_TYPE_MAP = Arrays.asList(FieldType.values()).stream()
            .collect(Collectors.toMap(JDBCMetastore::toSql, a -> a));

    @Inject
    public JDBCMetastore(@Named("presto.metastore.jdbc") JDBCPoolDataSource dataSource, PrestoConfig config, EventBus eventBus, FieldDependencyBuilder.FieldDependency fieldDependency) {
        super(fieldDependency, eventBus);
        this.config = config;

        this.prestoConnectionFactory = () -> {
            Properties properties = new Properties();
            properties.put("user", "presto-rakam");
            return DriverManager.getConnection(String.format("jdbc:presto://%s:%d",
                    config.getAddress().getHost(), config.getAddress().getPort()), properties);
        };
        dbi = new DBI(dataSource);

        schemaCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build(new CacheLoader<ProjectCollection, List<SchemaField>>() {
            @Override
            public List<SchemaField> load(ProjectCollection key) throws Exception {
                try (Connection conn = prestoConnectionFactory.openConnection()) {
                    List<SchemaField> schema = getSchema(conn, key.project, key.collection);
                    if (schema == null) {
                        return ImmutableList.of();
                    }
                    return schema;
                }
            }
        });

        collectionCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build(new CacheLoader<String, Set<String>>() {
            @Override
            public Set<String> load(String project) throws Exception {
                try (Connection conn = prestoConnectionFactory.openConnection()) {
                    HashSet<String> tables = new HashSet<>();

                    ResultSet tableRs = conn.getMetaData().getTables(config.getColdStorageConnector(), project, null, new String[]{"TABLE"});
                    while (tableRs.next()) {
                        String tableName = tableRs.getString("table_name");

                        if (!tableName.startsWith(PrestoMaterializedViewService.MATERIALIZED_VIEW_PREFIX)) {
                            tables.add(tableName);
                        }
                    }

                    return tables;
                }
            }
        });

        apiKeyCache = CacheBuilder.newBuilder().build(new CacheLoader<String, List<Set<String>>>() {
            @Override
            public List<Set<String>> load(String project) throws Exception {
                try (Handle handle = dbi.open()) {

                    Set<String> masterKeyList = new HashSet<>();
                    Set<String> readKeyList = new HashSet<>();
                    Set<String> writeKeyList = new HashSet<>();

                    Set<String>[] keys =
                            Arrays.stream(AccessKeyType.values()).map(key -> new HashSet<String>()).toArray(Set[]::new);

                    PreparedStatement ps = handle.getConnection().prepareStatement("SELECT master_key, read_key, write_key from api_key WHERE project = ?");
                    ps.setString(1, project);
                    ResultSet resultSet = ps.executeQuery();
                    while (resultSet.next()) {
                        String apiKey;

                        apiKey = resultSet.getString(1);
                        if (apiKey != null) {
                            masterKeyList.add(apiKey);
                        }
                        apiKey = resultSet.getString(2);
                        if (apiKey != null) {
                            readKeyList.add(apiKey);
                        }
                        apiKey = resultSet.getString(3);
                        if (apiKey != null) {
                            writeKeyList.add(apiKey);
                        }
                    }

                    keys[AccessKeyType.MASTER_KEY.ordinal()] = Collections.unmodifiableSet(masterKeyList);
                    keys[AccessKeyType.READ_KEY.ordinal()] = Collections.unmodifiableSet(readKeyList);
                    keys[AccessKeyType.WRITE_KEY.ordinal()] = Collections.unmodifiableSet(writeKeyList);

                    return Collections.unmodifiableList(Arrays.asList(keys));
                }
            }
        });


    }

    @PostConstruct
    public void setup() {
        setupTables();
        super.checkExistingSchema();
    }

    private void setupTables() {
        dbi.inTransaction((Handle handle, TransactionStatus transactionStatus) -> {
            handle.createStatement("CREATE TABLE IF NOT EXISTS api_key (" +
                    "  id SERIAL PRIMARY KEY,\n" +
                    "  project TEXT NOT NULL,\n" +
                    "  read_key TEXT NOT NULL,\n" +
                    "  write_key TEXT NOT NULL,\n" +
                    "  master_key TEXT NOT NULL,\n" +
                    "  created_at TIMESTAMP default current_timestamp NOT NULL\n" +
                    "  )").execute();
            handle.createStatement("CREATE TABLE IF NOT EXISTS project (" +
                    "  name TEXT NOT NULL,\n" +
                    "  PRIMARY KEY (name))")
                    .execute();
            return null;
        });
    }

    @Override
    public Map<String, Set<String>> getAllCollections() {
        return Collections.unmodifiableMap(collectionCache.asMap());
    }

    @Override
    public Map<String, List<SchemaField>> getCollections(String project) {
        try {
            return collectionCache.get(project).stream()
                    .collect(Collectors.toMap(c -> c, collection ->
                            getCollection(project, collection)));
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Set<String> getCollectionNames(String project) {
        try {
            return collectionCache.get(project);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ProjectApiKeys createApiKeys(String project) {
        String masterKey = CryptUtil.generateRandomKey(64);
        String readKey = CryptUtil.generateRandomKey(64);
        String writeKey = CryptUtil.generateRandomKey(64);

        int id;
        try (Handle handle = dbi.open()) {
            id = handle.createStatement("INSERT INTO api_key (project, master_key, read_key, write_key) VALUES (:project, :master_key, :read_key, :write_key)")
                    .bind("project", project)
                    .bind("master_key", masterKey)
                    .bind("read_key", readKey)
                    .bind("write_key", writeKey)
                    .executeAndReturnGeneratedKeys(IntegerMapper.FIRST).first();
        }

        return new ProjectApiKeys(id, project, masterKey, readKey, writeKey);
    }

    @Override
    public void revokeApiKeys(String project, int id) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM api_key WHERE project = :project AND id = :id")
                    .bind("project", project)
                    .bind("id", id)
                    .execute();
        }
    }

    @Override
    public void createProject(String project) {
        checkProject(project);

        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO project (name) VALUES(:name)")
                    .bind("name", project)
                    .execute();
        }

        super.onCreateProject(project);
    }

    @Override
    public Set<String> getProjects() {
        try (Handle handle = dbi.open()) {
            return ImmutableSet.copyOf(
                    handle.createQuery("select name from project")
                            .map(StringMapper.FIRST).iterator());
        }
    }

    @Override
    public List<SchemaField> getCollection(String project, String collection) {
        try {
            return schemaCache.get(new ProjectCollection(project, collection));
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    public static List<SchemaField> convertToSchema(ResultSet dbColumns) throws SQLException {
        List<SchemaField> schemaFields = Lists.newArrayList();

        while (dbColumns.next()) {
            String columnName = dbColumns.getString("COLUMN_NAME");
            FieldType fieldType;
            fieldType = fromSql(dbColumns.getInt("DATA_TYPE"), dbColumns.getString("TYPE_NAME"), JDBCMetastore::getType);
            schemaFields.add(new SchemaField(columnName, fieldType));
        }
        return schemaFields.size() == 0 ? null : schemaFields;
    }

    public static FieldType getType(String name) {
        FieldType fieldType = REVERSE_TYPE_MAP.get(name.toUpperCase());
        Objects.requireNonNull(fieldType, String.format("type %s couldn't recognized.", name));
        return fieldType;
    }

    private List<SchemaField> getSchema(Connection connection, String project, String collection) throws SQLException {
        ResultSet dbColumns = connection.getMetaData().getColumns(config.getColdStorageConnector(), project, collection, null);
        return convertToSchema(dbColumns);
    }

    @Override
    public synchronized List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields) throws NotExistsException {
        if (!collection.matches("^[a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException("Only alphanumeric characters allowed in collection name.");
        }

        List<SchemaField> currentFields = new ArrayList<>();
        String query;
        try (Connection connection = prestoConnectionFactory.openConnection()) {
            ResultSet columns = connection.getMetaData().getColumns(config.getColdStorageConnector(), project, collection, null);
            HashSet<String> strings = new HashSet<>();
            while (columns.next()) {
                String colName = columns.getString("COLUMN_NAME");
                strings.add(colName);
                currentFields.add(new SchemaField(colName, fromSql(columns.getInt("DATA_TYPE"), columns.getString("TYPE_NAME"), JDBCMetastore::getType)));
            }

            List<SchemaField> schemaFields = fields.stream().filter(f -> !strings.contains(f.getName())).collect(Collectors.toList());
            Runnable task;
            if (currentFields.size() == 0) {
                if (!getProjects().contains(project)) {
                    throw new NotExistsException("project", HttpResponseStatus.UNAUTHORIZED);
                }
                String queryEnd = schemaFields.stream()
                        .map(f -> {
                            currentFields.add(f);
                            return f;
                        })
                        .map(f -> format("\"%s\" %s", f.getName(), toSql(f.getType())))
                        .collect(Collectors.joining(", "));
                if (queryEnd.isEmpty()) {
                    return currentFields;
                }
                // WITH (temporal_column = '_time', ordering = ARRAY['_time'])
                query = format("CREATE TABLE %s.\"%s\".\"%s\" (%s)", config.getColdStorageConnector(), project, collection, queryEnd);
                connection.createStatement().execute(query);

                task = () -> super.onCreateCollection(project, collection, schemaFields);
            } else {
                schemaFields.stream()
                        .map(f -> {
                            currentFields.add(f);
                            return f;
                        })
                        .map(f -> format("ALTER TABLE %s.\"%s\".\"%s\" ADD COLUMN \"%s\" %s",
                                config.getColdStorageConnector(), project, collection,
                                f.getName(), toSql(f.getType())))
                        .forEach(q -> {
                            try {
                                connection.createStatement().execute(q);
                            } catch (SQLException e) {
                                throw Throwables.propagate(e);
                            }
                        });

                task = () -> super.onCreateCollectionField(project, collection, schemaFields);
            }

            task.run();
            schemaCache.put(new ProjectCollection(project, collection), currentFields);
            return currentFields;
        } catch (SQLException e) {
            // column or table already exists
            if (e.getMessage().contains("exists")) {
                // TODO: should we try again until this operation is done successfully, what about infinite loops?
                return getOrCreateCollectionFieldList(project, collection, fields);
            } else {
                throw new IllegalStateException(e.getMessage());
            }
        }
    }

    @Override
    public boolean checkPermission(String project, AccessKeyType type, String apiKey) {
        try {
            boolean exists = apiKeyCache.get(project).get(type.ordinal()).contains(apiKey);
            if (!exists) {
                apiKeyCache.refresh(project);
                return apiKeyCache.get(project).get(type.ordinal()).contains(apiKey);
            }
            return true;
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<ProjectApiKeys> getApiKeys(int[] ids) {
        if (ids.length == 0) {
            return ImmutableList.of();
        }
        try (Handle handle = dbi.open()) {
            Connection conn = handle.getConnection();
            PreparedStatement ps = conn.prepareStatement("select id, project, master_key, read_key, write_key from api_key where id = any(?)");
            ps.setArray(1, conn.createArrayOf("int4", Arrays.stream(ids).boxed().toArray()));
            ResultSet resultSet = ps.executeQuery();
            ArrayList<ProjectApiKeys> list = new ArrayList<>();
            while (resultSet.next()) {
                list.add(new ProjectApiKeys(resultSet.getInt(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5)));
            }
            return list;
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void deleteProject(String project) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("delete from api_key where project = :project")
                    .bind("project", project).execute();
            handle.createStatement("delete from project where name = :project")
                    .bind("project", project).execute();
        }

        Set<String> collectionNames = getCollectionNames(project);
        try (Connection connection = prestoConnectionFactory.openConnection()) {
            Statement statement = connection.createStatement();

            while(!collectionNames.isEmpty()) {
                for (String collectionName : collectionNames) {
                    statement.execute(String.format("drop table %s.%s.%s",
                            config.getColdStorageConnector(), project, collectionName));
                }

                collectionCache.refresh(project);
                collectionNames = collectionCache.getUnchecked(project);
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public static String toSql(FieldType type) {
        switch (type) {
            case LONG:
                return "BIGINT";
            case STRING:
                return "VARCHAR";
            case BINARY:
                return "VARBINARY";
            case BOOLEAN:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return type.name();
            case DOUBLE:
                return "DOUBLE";
            default:
                if (type.isArray()) {
                    return "ARRAY<" + toSql(type.getArrayElementType()) + ">";
                }
                if(type.isMap()) {
                    return "MAP<VARCHAR, "+ toSql(type.getMapValueType()) +">";
                }
                throw new IllegalStateException("sql type couldn't converted to fieldtype");
        }
    }

    @VisibleForTesting
    public void destroy() {
        clearCache();
        getProjects().forEach(this::deleteProject);

        try (Handle handle = dbi.open()) {
            handle.execute("drop table api_key");
            handle.execute("drop table project");
        }
    }

    @VisibleForTesting
    public void clearCache() {
        collectionCache.cleanUp();
        apiKeyCache.cleanUp();
        schemaCache.cleanUp();
    }

}
