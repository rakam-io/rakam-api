package org.rakam.presto.analysis;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
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
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.AbstractMetastore;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.util.JDBCUtil;
import org.rakam.util.NotExistsException;
import org.rakam.util.ProjectCollection;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.ConnectionFactory;
import org.skife.jdbi.v2.util.StringMapper;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.rakam.util.JDBCUtil.fromSql;
import static org.rakam.util.JDBCUtil.toSql;
import static org.rakam.util.ValidationUtil.checkProject;

@Singleton
public class JDBCMetastore extends AbstractMetastore {
    private final DBI dbi;
    private final LoadingCache<ProjectCollection, List<SchemaField>> schemaCache;
    private final LoadingCache<String, Set<String>> collectionCache;
    private final ConnectionFactory prestoConnectionFactory;
    private final PrestoConfig config;

    @Inject
    public JDBCMetastore(@Named("presto.metastore.jdbc") JDBCPoolDataSource dataSource, PrestoConfig config, EventBus eventBus) {
        super(eventBus);
        this.config = config;

        this.prestoConnectionFactory = () -> {
            Properties properties = new Properties();
            properties.put("user", "presto-rakam");
            return DriverManager.getConnection(String.format("jdbc:presto://%s:%d",
                    config.getAddress().getHost(), config.getAddress().getPort()), properties);
        };
        dbi = new DBI(dataSource);

        schemaCache = CacheBuilder.newBuilder().expireAfterWrite(20, TimeUnit.MINUTES)
                .build(new CacheLoader<ProjectCollection, List<SchemaField>>() {
                    @Override
                    public List<SchemaField> load(ProjectCollection key) throws Exception {
                        try (Connection conn = prestoConnectionFactory.openConnection()) {
                            ResultSet dbColumns = conn.getMetaData().getColumns(config.getColdStorageConnector(), key.project, key.collection, null);
                            List<SchemaField> schema = convertToSchema(dbColumns);

                            if (schema == null) {
                                return ImmutableList.of();
                            }
                            return schema;
                        }
                    }
                });

        collectionCache = CacheBuilder.newBuilder().expireAfterWrite(20, TimeUnit.MINUTES).build(new CacheLoader<String, Set<String>>() {
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

    }

    @PostConstruct
    public void setup() {
        setupTables();
    }

    private void setupTables() {
        dbi.inTransaction((Handle handle, TransactionStatus transactionStatus) -> {
            handle.createStatement("CREATE TABLE IF NOT EXISTS project (" +
                    "  name TEXT NOT NULL,\n" +
                    "  PRIMARY KEY (name))")
                    .execute();
            return null;
        });
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
            fieldType = fromSql(dbColumns.getInt("DATA_TYPE"), dbColumns.getString("TYPE_NAME"));
            schemaFields.add(new SchemaField(columnName, fieldType));
        }
        return schemaFields.isEmpty() ? null : schemaFields;
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
                currentFields.add(new SchemaField(colName, fromSql(columns.getInt("DATA_TYPE"), columns.getString("TYPE_NAME"))));
            }

            List<SchemaField> schemaFields = fields.stream().filter(f -> !strings.contains(f.getName())).collect(Collectors.toList());
            Runnable task;
            if (currentFields.isEmpty()) {
                if (!getProjects().contains(project)) {
                    throw new NotExistsException("Project");
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
                query = format("CREATE TABLE %s.\"%s\".\"%s\" (%s) WITH (temporal_column = '_time') ", config.getColdStorageConnector(), project, collection, queryEnd);
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
    public void deleteProject(String project) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("delete from project where name = :project")
                    .bind("project", project).execute();
        }

        Set<String> collectionNames = getCollectionNames(project);
        try (Connection connection = prestoConnectionFactory.openConnection()) {
            Statement statement = connection.createStatement();

            while (!collectionNames.isEmpty()) {
                for (String collectionName : collectionNames) {
                    statement.execute(String.format("drop table %s.%s.%s",
                            config.getColdStorageConnector(), project, collectionName));
                    schemaCache.invalidate(new ProjectCollection(project, collectionName));
                }

                collectionCache.refresh(project);
                collectionNames = collectionCache.getUnchecked(project);
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }

        collectionCache.invalidate(project);
        super.onDeleteProject(project);
    }

    @VisibleForTesting
    public void clearCache() {
        collectionCache.cleanUp();
        schemaCache.cleanUp();
    }
}
