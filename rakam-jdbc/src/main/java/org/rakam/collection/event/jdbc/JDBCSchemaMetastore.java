package org.rakam.collection.event.jdbc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.rakam.JDBCConfig;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/02/15 15:57.
 */
@Singleton
public class JDBCSchemaMetastore implements EventSchemaMetastore {

    private final Handle dao;
    private final DBI dbi;

    @Inject
    public JDBCSchemaMetastore(@Named("event.schema.store.jdbc") JDBCConfig config) {
        dbi = new DBI(String.format(config.getUrl(), config.getUsername(), config.getPassword()),
                config.getUsername(), config.getUsername());
        dao = dbi.open();

        dao.createStatement("CREATE TABLE IF NOT EXISTS collection_schema (" +
                "  project VARCHAR(255) NOT NULL,\n" +
                "  collection VARCHAR(255) NOT NULL,\n" +
                "  schema TEXT NOT NULL,\n" +
                "  PRIMARY KEY (project, collection)"+
                ")")
                .execute();
    }

    @Override
    public Map<String, List<String>> getAllCollections() {
        Query<Map<String, Object>> bind = dao.createQuery("SELECT project, collection from collection_schema");

        Map<String, List<String>> table = Maps.newHashMap();
        bind.forEach(row ->
                table.computeIfAbsent((String) row.get("project"),
                        (key) -> Lists.newArrayList()).add((String) row.get("collection")));

        return table;
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project) {
        Query<Map<String, Object>> bind = dao.createQuery("SELECT collection, schema from collection_schema WHERE project = :project")
                .bind("project", project);

        HashMap<String, List<SchemaField>> table = Maps.newHashMap();
        bind.forEach(row ->
                table.put((String) row.get("collection"), Arrays.asList(JsonHelper.read((String) row.get("schema"), SchemaField[].class))));
        return table;
    }

    @Override
    public List<SchemaField> getSchema(String project, String collection) {
        return getSchema(dao, project, collection);
    }

    private List<SchemaField> getSchema(Handle dao, String project, String collection) {
        Query<Map<String, Object>> bind = dao.createQuery("SELECT schema from collection_schema WHERE project = :project AND collection = :collection")
                .bind("project", project)
                .bind("collection", collection);
        Map<String, Object> o = bind.first();
        return o != null ? Arrays.asList(JsonHelper.read((String) o.get("schema"), SchemaField[].class)) : null;
    }

    @Override
    public List<SchemaField> createOrGetSchema(String project, String collection, List<SchemaField> newFields) {
        return dbi.inTransaction((dao, status) -> {
            List<SchemaField> fields = getSchema(dao, project, collection);
            if(fields == null) {
                dao.createStatement("INSERT INTO collection_schema (project, collection, schema) VALUES (:project, :collection, :schema)")
                        .bind("schema", JsonHelper.encodeAsBytes(newFields))
                        .bind("project", project)
                        .bind("collection", collection)
                        .execute();
                return newFields;
            } else {
                List<SchemaField> _newFields = Lists.newArrayList();

                for (SchemaField field : newFields) {
                    Optional<SchemaField> first = fields.stream().filter(x -> x.getName().equals(field.getName())).findFirst();
                    if(!first.isPresent()) {
                        _newFields.add(field);
                    } else {
                        if(first.get().getType() != field.getType())
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
    }
}
