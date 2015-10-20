package org.rakam.analysis;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.EventMapper;
import org.rakam.report.PrestoConfig;
import org.rakam.util.CryptUtil;
import org.rakam.util.JsonHelper;
import org.rakam.util.ProjectCollection;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.util.StringMapper;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.rakam.util.ValidationUtil.checkProject;

@Singleton
public class JDBCMetastore extends AbstractMetastore {
    private final DBI dbi;
    private final PrestoConfig prestoConfig;
    private final LoadingCache<ProjectCollection, List<SchemaField>> schemaCache;
    private final LoadingCache<String, Set<String>> collectionCache;
    private final LoadingCache<String, List<Set<String>>> apiKeyCache;

    @Inject
    public JDBCMetastore(@Named("presto.metastore.jdbc") JDBCPoolDataSource dataSource, EventBus eventBus, Set<EventMapper> eventMappers, PrestoConfig prestoConfig) {
        super(eventMappers, eventBus);
        this.prestoConfig = prestoConfig;

        dbi = new DBI(dataSource);

        schemaCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build(new CacheLoader<ProjectCollection, List<SchemaField>>() {
            @Override
            public List<SchemaField> load(ProjectCollection key) throws Exception {
                try(Handle handle = dbi.open()) {
                    List<SchemaField> schema = getSchema(handle, key.project, key.collection);
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
                try(Handle handle = dbi.open()) {
                    return StreamSupport.stream(handle.createQuery("SELECT collection from collection_schema WHERE project = :project")
                            .bind("project", project).map(StringMapper.FIRST).spliterator(), false).collect(Collectors.toSet());
                }
            }
        });

        apiKeyCache = CacheBuilder.newBuilder().build(new CacheLoader<String, List<Set<String>>>() {
            @Override
            public List<Set<String>> load(String project) throws Exception {
                try(Handle handle = dbi.open()) {

                    Set<String> masterKeyList = new HashSet<>();
                    Set<String> readKeyList = new HashSet<>();
                    Set<String> writeKeyList = new HashSet<>();

                    Set<String>[] keys =
                            Arrays.stream(AccessKeyType.values()).map(key -> new HashSet<String>()).toArray(Set[]::new);

                    PreparedStatement ps = handle.getConnection().prepareStatement("SELECT master_key, read_key, write_key from api_key WHERE project = :project");
                    ps.setString(1, project);
                    ResultSet resultSet = ps.executeQuery();
                    while(resultSet.next()) {
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

        setup();
        super.checkExistingSchema();
    }

    private void setup() {
        dbi.inTransaction((Handle handle, TransactionStatus transactionStatus) -> {
            handle.createStatement("CREATE TABLE IF NOT EXISTS collection_schema (" +
                    "  project VARCHAR(255) NOT NULL,\n" +
                    "  collection VARCHAR(255) NOT NULL,\n" +
                    "  schema TEXT NOT NULL,\n" +
                    "  PRIMARY KEY (project, collection)"+
                    ")")
                    .execute();

            handle.createStatement("CREATE TABLE IF NOT EXISTS project (" +
                    "  name TEXT NOT NULL,\n" +
                    "  location TEXT NOT NULL, PRIMARY KEY (name))")
                    .execute();

            handle.createStatement("CREATE TABLE IF NOT EXISTS api_key (" +
                    "  id SERIAL PRIMARY KEY,\n" +
                    "  project TEXT NOT NULL,\n" +
                    "  read_key TEXT NOT NULL,\n" +
                    "  write_key TEXT NOT NULL,\n" +
                    "  secret_key TEXT NOT NULL,\n" +
                    "  created_at TIMESTAMP default current_timestamp NOT NULL\n"+
                    "  )").execute();
            return null;
        });
    }

    public String getDatabaseLocation(String project) {
        try(Handle handle = dbi.open()) {
            return handle.createQuery("SELECT location from project WHERE name = :name")
                    .bind("name", project).map(StringMapper.FIRST)
                    .first();
        }
    }

    @Override
    public Map<String, Set<String>> getAllCollections() {
        return Collections.unmodifiableMap(collectionCache.asMap());
    }

    @Override
    public Map<String, List<SchemaField>> getCollections(String project) {
        try(Handle handle = dbi.open()) {
            Query<Map<String, Object>> bind = handle.createQuery("SELECT collection, schema from collection_schema WHERE project = :project")
                    .bind("project", project);

            HashMap<String, List<SchemaField>> table = Maps.newHashMap();
            bind.forEach(row ->
                    table.put((String) row.get("collection"), Arrays.asList(JsonHelper.read((String) row.get("schema"), SchemaField[].class))));
            return table;
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
    public ProjectApiKeyList createApiKeys(String project) {
        String masterKey = CryptUtil.generateKey(64);
        String readKey = CryptUtil.generateKey(64);
        String writeKey = CryptUtil.generateKey(64);

        try(Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO api_key (project, master_key, read_key, write_key) VALUES(:project, :location, :master_key, :read_key, :write_key)")
                    .bind("project", project)
                    .bind("master_key", masterKey)
                    .bind("read_key", readKey)
                    .bind("write_key", writeKey)
                    .execute();
        }

        return new ProjectApiKeyList(masterKey, readKey, writeKey);
    }

    @Override
    public void createProject(String project) {
        checkProject(project);

        try(Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO project (name, location, master_key, read_key, write_key) VALUES(:name, :location, :master_key, :read_key, :write_key)")
                    .bind("name", project)
                     // todo: file.separator returns local filesystem property?
                    .bind("location", prestoConfig.getStorage().replaceFirst("/+$", "") + File.separator + project + File.separator)
                    .execute();
        }

        super.onCreateProject(project);
    }

    @Override
    public Set<String> getProjects() {
        try(Handle handle = dbi.open()) {
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

    @Nullable
    private List<SchemaField> getSchema(Handle dao, String project, String collection) {
        Query<Map<String, Object>> bind = dao.createQuery("SELECT schema from collection_schema WHERE project = :project AND collection = :collection")
                .bind("project", project)
                .bind("collection", collection);
        Map<String, Object> o = bind.first();
        return o != null ? Arrays.asList(JsonHelper.read((String) o.get("schema"), SchemaField[].class)) : null;
    }

    @Override
    public synchronized List<SchemaField> getOrCreateCollectionFields(String project, String collection, List<SchemaField> newFields) throws ProjectNotExistsException {
        List<SchemaField> schemaFields;
        try {
            schemaFields = dbi.inTransaction((dao, status) -> {
                List<SchemaField> schema = getSchema(dao, project, collection);

                if (schema == null) {
                    if(!getProjects().contains(project)) {
                        throw new ProjectNotExistsException();
                    }
                    dao.createStatement("INSERT INTO collection_schema (project, collection, schema) VALUES (:project, :collection, :schema)")
                            .bind("schema", JsonHelper.encode(newFields))
                            .bind("project", project)
                            .bind("collection", collection)
                            .execute();
                    super.onCreateCollection(project, collection);
                    return newFields;
                } else {
                    List<SchemaField> fields = Lists.newCopyOnWriteArrayList(schema);

                    List<SchemaField> _newFields = Lists.newArrayList();

                    for (SchemaField field : newFields) {
                        Optional<SchemaField> first = fields.stream().filter(x -> x.getName().equals(field.getName())).findFirst();
                        if (!first.isPresent()) {
                            _newFields.add(field);
                        } else {
                            if (first.get().getType() != field.getType())
                                throw new IllegalArgumentException();
                        }
                    }

                    fields.addAll(_newFields);
                    dao.createStatement("UPDATE collection_schema SET schema = :schema WHERE project = :project AND collection = :collection")
                            .bind("schema", JsonHelper.encode(fields))
                            .bind("project", project)
                            .bind("collection", collection)
                            .execute();
                    return fields;
                }

            });
        } catch (CallbackFailedException e) {
            // TODO: find a better way to handle this.
            if(e.getCause() instanceof ProjectNotExistsException) {
                throw (ProjectNotExistsException) e.getCause();
            }
            if(e.getCause() instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) e.getCause();
            } else {
                throw e;
            }
        }
        schemaCache.refresh(new ProjectCollection(project, collection));
        collectionCache.refresh(project);
        return schemaFields;
    }

    @Override
    public boolean checkPermission(String project, AccessKeyType type, String apiKey) {
        try {
            boolean exists = apiKeyCache.get(project).get(type.ordinal()).contains(apiKey);
            if(!exists) {
                apiKeyCache.refresh(project);
                return apiKeyCache.get(project).get(type.ordinal()).contains(apiKey);
            }
            return true;
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }
}
