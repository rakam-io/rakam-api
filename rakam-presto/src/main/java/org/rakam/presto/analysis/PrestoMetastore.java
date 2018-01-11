package org.rakam.presto.analysis;

import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.util.NotExistsException;
import org.rakam.util.ProjectCollection;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;
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
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.ParameterKind.TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static java.lang.String.format;
import static org.rakam.util.JDBCUtil.fromSql;
import static org.rakam.util.JDBCUtil.toSql;
import static org.rakam.util.ValidationUtil.checkLiteral;
import static org.rakam.util.ValidationUtil.checkProject;

public class PrestoMetastore
        extends PrestoAbstractMetastore {
    protected final LoadingCache<ProjectCollection, List<SchemaField>> schemaCache;
    protected final LoadingCache<String, Set<String>> collectionCache;
    protected final ConnectionFactory prestoConnectionFactory;
    protected final PrestoConfig config;
    private final DBI dbi;
    private final ProjectConfig projectConfig;

    @Inject
    public PrestoMetastore(ProjectConfig projectConfig, @Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource, PrestoConfig config, EventBus eventBus) {
        super(eventBus);
        dbi = new DBI(dataSource);

        this.config = config;
        this.projectConfig = projectConfig;

        this.prestoConnectionFactory = () -> {
            Properties properties = new Properties();
            properties.put("user", "presto-rakam");
            return DriverManager.getConnection(String.format("jdbc:presto://%s:%d",
                    config.getAddress().getHost(), config.getAddress().getPort()), properties);
        };

        schemaCache = CacheBuilder.newBuilder().expireAfterWrite(20, TimeUnit.MINUTES)
                .build(new CacheLoader<ProjectCollection, List<SchemaField>>() {
                    @Override
                    public List<SchemaField> load(ProjectCollection key)
                            throws Exception {
                        try (Connection conn = prestoConnectionFactory.openConnection()) {
                            ResultSet dbColumns = conn.getMetaData().getColumns(config.getColdStorageConnector(),
                                    key.project, key.collection, null);
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
            public Set<String> load(String project)
                    throws Exception {
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
        dbi.inTransaction((Handle handle, TransactionStatus transactionStatus) -> {
            handle.createStatement("CREATE TABLE IF NOT EXISTS project (" +
                    "  name TEXT NOT NULL,\n" +
                    "  PRIMARY KEY (name))")
                    .execute();
            return null;
        });
    }

    @Override
    public void createProject(String project) {
        if (config.getExistingProjects().contains(project)) {
            checkProject(project);

            try (Handle handle = dbi.open()) {
                handle.createStatement("INSERT INTO project (name) VALUES(:name)")
                        .bind("name", project)
                        .execute();
            } catch (Exception e) {
                if (getProjects().contains(project)) {
                    throw new RakamException("The project already exists", BAD_REQUEST);
                }
            }

            super.onCreateProject(project);
            return;
        }

        throw new RakamException(NOT_IMPLEMENTED);
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
    public void deleteProject(String project) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("delete from project where name = :project")
                    .bind("project", project).execute();
        }
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project, Predicate<String> filter) {
        Map<String, List<SchemaField>> map = new HashMap<>();

        collectionCache.getUnchecked(project).stream()
                .filter(table -> filter.test(table))
                .forEach(table -> map.put(table, schemaCache.getUnchecked(new ProjectCollection(project, table))));

        return map;
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
    public List<SchemaField> getCollection(String project, String collection) {
        try {
            return schemaCache.get(new ProjectCollection(project, collection));
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    public List<SchemaField> convertToSchema(ResultSet dbColumns)
            throws SQLException {
        List<SchemaField> schemaFields = Lists.newArrayList();

        while (dbColumns.next()) {
            String columnName = dbColumns.getString("COLUMN_NAME");
            FieldType fieldType = fromSql(dbColumns.getInt("DATA_TYPE"), dbColumns.getString("TYPE_NAME"), s -> fromPrestoType(s));
            schemaFields.add(new SchemaField(columnName, fieldType));
        }
        return schemaFields.isEmpty() ? null : schemaFields;
    }

    public FieldType fromPrestoType(String name) {
        TypeSignature typeSignature = TypeSignature.parseTypeSignature(name);

        return PrestoQueryExecution.fromPrestoType(typeSignature.getBase(),
                typeSignature.getParameters().stream()
                        .filter(param -> param.getKind() == TYPE)
                        .map(param -> param.getTypeSignature().getBase()).iterator());
    }

    @Override
    public synchronized List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields)
            throws NotExistsException {
        ValidationUtil.checkCollectionValid(collection);

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
                currentFields.add(new SchemaField(colName, fromSql(columns.getInt("DATA_TYPE"), columns.getString("TYPE_NAME"), this::fromPrestoType)));
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
                query = format("CREATE TABLE %s.\"%s\".\"%s\" (%s) WITH (temporal_column = '%s') ", config.getColdStorageConnector(), project, collection, queryEnd, checkLiteral(projectConfig.getTimeColumn()));
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
                return getOrCreateCollectionFields(project, collection, fields);
            } else {
                throw new IllegalStateException(e.getMessage());
            }
        }
    }

    @Override
    public CompletableFuture<List<String>> getAttributes(String project, String collection, String attribute, Optional<LocalDate> startDate,
                                                         Optional<LocalDate> endDate, Optional<String> filter) {
        throw new UnsupportedOperationException();
    }

    @VisibleForTesting
    public void clearCache() {
        collectionCache.cleanUp();
        schemaCache.cleanUp();
    }


}
