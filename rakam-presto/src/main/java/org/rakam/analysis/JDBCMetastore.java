package org.rakam.analysis;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.JDBCConfig;
import org.rakam.report.PrestoConfig;
import org.rakam.util.JsonHelper;
import org.rakam.util.ProjectCollection;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Singleton
public class JDBCMetastore implements Metastore {
    private final Handle dao;
    private final DBI dbi;
    private final PrestoConfig prestoConfig;
    private final LoadingCache<ProjectCollection, List<SchemaField>> schemaCache;
    private final LoadingCache<String, Set<String>> collectionCache;

    @Inject
    public JDBCMetastore(@Named("presto.metastore.jdbc") JDBCConfig config, PrestoConfig prestoConfig) {
        this.prestoConfig = prestoConfig;
        dbi = new DBI(config.getUrl(), config.getUsername(), config.getPassword());
        dao = dbi.open();

        schemaCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build(new CacheLoader<ProjectCollection, List<SchemaField>>() {
            @Override
            public List<SchemaField> load(ProjectCollection key) throws Exception {
                List<SchemaField> schema = getSchema(dao, key.project, key.collection);
                if(schema == null) {
                    return ImmutableList.of();
                }
                return schema;
            }
        });

        collectionCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build(new CacheLoader<String, Set<String>>() {
            @Override
            public Set<String> load(String project) throws Exception {
                Query<Map<String, Object>> bind = dao.createQuery("SELECT collection from collection_schema WHERE project = :project")
                        .bind("project", project);

                return bind.list().stream().map(row ->
                        (String) row.get("collection")).collect(Collectors.toSet());
            }
        });

        dao.createStatement("CREATE TABLE IF NOT EXISTS collection_schema (" +
                "  project VARCHAR(255) NOT NULL,\n" +
                "  collection VARCHAR(255) NOT NULL,\n" +
                "  schema TEXT NOT NULL,\n" +
                "  PRIMARY KEY (project, collection)"+
                ")")
                .execute();

        dao.createStatement("CREATE TABLE IF NOT EXISTS cold_storage_table_metadata (" +
                "  databaseName VARCHAR(255) NOT NULL,\n" +
                "  tableName VARCHAR(255) NOT NULL,\n" +
                "  metadata TEXT NOT NULL, PRIMARY KEY (databaseName, tableName))")
                .execute();

        dao.createStatement("CREATE TABLE IF NOT EXISTS cold_storage_database_metadata (" +
                "  databaseName VARCHAR(255) NOT NULL,\n" +
                "  metadata TEXT NOT NULL, PRIMARY KEY (databaseName))")
                .execute();
    }

    @Override
    public Map<String, Collection<String>> getAllCollections() {
        Query<Map<String, Object>> bind = dao.createQuery("SELECT project, collection from collection_schema");

        Map<String, Collection<String>> table = Maps.newHashMap();
        bind.forEach(row ->
                table.computeIfAbsent((String) row.get("project"),
                        (key) -> Lists.newArrayList()).add((String) row.get("collection")));

        return table;
    }

    @Override
    public Map<String, List<SchemaField>> getCollections(String project) {
        Query<Map<String, Object>> bind = dao.createQuery("SELECT collection, schema from collection_schema WHERE project = :project")
                .bind("project", project);

        HashMap<String, List<SchemaField>> table = Maps.newHashMap();
        bind.forEach(row ->
                table.put((String) row.get("collection"), Arrays.asList(JsonHelper.read((String) row.get("schema"), SchemaField[].class))));
        return table;
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
        Database database = new Database(project, "Default Rakam Database", prestoConfig.getStorage()+ File.separator + project,
                Maps.newHashMap());

        TSerializer serializer = new TSerializer();
        byte[] data;
        try {
            data = serializer.serialize(database);
        }
        catch (TException e) {
            throw new RuntimeException();
        }

        dao.createStatement("INSERT INTO cold_storage_database_metadata (databaseName, metadata) VALUES(:databaseName, :metadata)")
                .bind("databaseName", database.getName())
                .bind("metadata", data)
                .execute();
    }

    @Override
    public Set<String> getProjects() {
        return ImmutableSet.copyOf(
                dao.createQuery("select distinct project from collection_schema")
                        .map((index, r, ctx) -> r.getString(1)).iterator());
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
    public synchronized List<SchemaField> createOrGetCollectionField(String project, String collection, List<SchemaField> newFields) {
        List<SchemaField> schemaFields = dbi.inTransaction((dao, status) -> {
            List<SchemaField> schema = getSchema(dao, project, collection);

            if (schema == null) {
                dao.createStatement("INSERT INTO collection_schema (project, collection, schema) VALUES (:project, :collection, :schema)")
                        .bind("schema", JsonHelper.encode(newFields))
                        .bind("project", project)
                        .bind("collection", collection)
                        .execute();
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
        schemaCache.refresh(new ProjectCollection(project, collection));
        collectionCache.refresh(project);
        return schemaFields;
    }
}
