package org.rakam.presto.analysis;

import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.inject.Singleton;
import org.rakam.analysis.metadata.AbstractMetastore;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.util.NotExistsException;
import org.rakam.util.ProjectCollection;
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.ParameterKind.TYPE;
import static java.lang.String.format;
import static org.rakam.util.JDBCUtil.fromSql;
import static org.rakam.util.JDBCUtil.toSql;
import static org.rakam.util.ValidationUtil.checkLiteral;

@Singleton
public abstract class PrestoAbstractMetastore
        extends AbstractMetastore
{
    protected final LoadingCache<ProjectCollection, List<SchemaField>> schemaCache;
    protected final LoadingCache<String, Set<String>> collectionCache;
    protected final ConnectionFactory prestoConnectionFactory;
    protected final PrestoConfig config;
    private final ProjectConfig projectConfig;

    @Inject
    public PrestoAbstractMetastore(ProjectConfig projectConfig, PrestoConfig config, EventBus eventBus)
    {
        super(eventBus);
        this.config = config;
        this.projectConfig = projectConfig;

        this.prestoConnectionFactory = () -> {
            Properties properties = new Properties();
            properties.put("user", "presto-rakam");
            return DriverManager.getConnection(String.format("jdbc:presto://%s:%d",
                    config.getAddress().getHost(), config.getAddress().getPort()), properties);
        };

        schemaCache = CacheBuilder.newBuilder().expireAfterWrite(20, TimeUnit.MINUTES)
                .build(new CacheLoader<ProjectCollection, List<SchemaField>>()
                {
                    @Override
                    public List<SchemaField> load(ProjectCollection key)
                            throws Exception
                    {
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

        collectionCache = CacheBuilder.newBuilder().expireAfterWrite(20, TimeUnit.MINUTES).build(new CacheLoader<String, Set<String>>()
        {
            @Override
            public Set<String> load(String project)
                    throws Exception
            {
                try (Connection conn = prestoConnectionFactory.openConnection()) {
                    HashSet<String> tables = new HashSet<>();

                    ResultSet tableRs = conn.getMetaData().getTables(config.getColdStorageConnector(), project, null, new String[] {"TABLE"});
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

    @Override
    public Map<String, List<SchemaField>> getCollections(String project)
    {
        try {
            return collectionCache.get(project).stream()
                    .collect(Collectors.toMap(c -> c, collection ->
                            getCollection(project, collection)));
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Set<String> getCollectionNames(String project)
    {
        try {
            return collectionCache.get(project);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<SchemaField> getCollection(String project, String collection)
    {
        try {
            return schemaCache.get(new ProjectCollection(project, collection));
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    public List<SchemaField> convertToSchema(ResultSet dbColumns)
            throws SQLException
    {
        List<SchemaField> schemaFields = Lists.newArrayList();

        while (dbColumns.next()) {
            String columnName = dbColumns.getString("COLUMN_NAME");
            FieldType fieldType;
            fieldType = fromSql(dbColumns.getInt("DATA_TYPE"), dbColumns.getString("TYPE_NAME"), s -> fromPrestoType(s));
            schemaFields.add(new SchemaField(columnName, fieldType));
        }
        return schemaFields.isEmpty() ? null : schemaFields;
    }

    public FieldType fromPrestoType(String name)
    {
        TypeSignature typeSignature = TypeSignature.parseTypeSignature(name);

        return PrestoQueryExecution.fromPrestoType(typeSignature.getBase(),
                typeSignature.getParameters().stream()
                        .filter(param -> param.getKind() == TYPE)
                        .map(param -> param.getTypeSignature().getBase()).iterator());
    }

    @Override
    public synchronized List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields)
            throws NotExistsException
    {
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
            }
            else {
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
                            }
                            catch (SQLException e) {
                                throw Throwables.propagate(e);
                            }
                        });

                task = () -> super.onCreateCollectionField(project, collection, schemaFields);
            }

            task.run();
            schemaCache.put(new ProjectCollection(project, collection), currentFields);
            return currentFields;
        }
        catch (SQLException e) {
            // column or table already exists
            if (e.getMessage().contains("exists")) {
                // TODO: should we try again until this operation is done successfully, what about infinite loops?
                return getOrCreateCollectionFieldList(project, collection, fields);
            }
            else {
                throw new IllegalStateException(e.getMessage());
            }
        }
    }

    @VisibleForTesting
    public void clearCache()
    {
        collectionCache.cleanUp();
        schemaCache.cleanUp();
    }
}
