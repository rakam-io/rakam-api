package org.rakam.collection.event.metastore;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.avro.Schema;
import org.rakam.collection.SchemaField;
import org.rakam.report.metadata.postgresql.PostgresqlConfig;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/02/15 15:57.
 */
@Singleton
public class PostgresqlSchemaMetastore implements EventSchemaMetastore {

    private final Handle dao;
    private final DBI dbi;

    @Inject
    public PostgresqlSchemaMetastore(@Named("event.schema.store.postgresql") PostgresqlConfig config) {
        dbi = new DBI(format("jdbc:postgresql://%s/%s", config.getHost(), config.getDatabase()),
                config.getUsername(), config.getUsername());
        dao = dbi.open();

        dao.createStatement("CREATE TABLE IF NOT EXISTS collection_schema (" +
                "  project VARCHAR(255) NOT NULL,\n" +
                "  collection VARCHAR(255) NOT NULL,\n" +
                "  schema BYTEA NOT NULL,\n" +
                "  PRIMARY KEY (project, collection)"+
                ")")
                .execute();
    }

    @Override
    public Table<String, String, Schema> getAllSchemas() {
        Query<Map<String, Object>> bind = dao.createQuery("SELECT project, collection, schema from collection_schema");

        HashBasedTable<String, String, Schema> table = HashBasedTable.create();
        bind.forEach(row ->
                table.put((String) row.get("project"), (String) row.get("collection"), (Schema) row.get("schema")));

        return table;
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
    public Map<String, Schema> getSchemas(String project) {
        Query<Map<String, Object>> bind = dao.createQuery("SELECT collection, schema from collection_schema WHERE project = :project")
                .bind("project", project);

        HashMap<String, Schema> table = Maps.newHashMap();
        bind.forEach(row ->
                table.put((String) row.get("collection"), new Schema.Parser().parse((String) row.get("schema"))));
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
        return o != null ? Arrays.asList(JsonHelper.read((byte[]) o.get("schema"), SchemaField[].class)) : null;
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
                fields = Lists.newArrayList(fields);

                for (SchemaField field : newFields) {
                    Optional<SchemaField> first = fields.stream().filter(x -> x.getName().equals(field.getName())).findFirst();
                    if(!first.isPresent()) {
                        fields.add(field);
                    } else {
                        if(first.get().getType() != field.getType())
                            throw new IllegalArgumentException();
                    }
                }

                dao.createStatement("UPDATE collection_schema SET schema = :schema WHERE project = :project AND collection = :collection")
                        .bind("schema", JsonHelper.encodeAsBytes(fields))
                        .bind("project", project)
                        .bind("collection", collection)
                        .execute();
                return fields;
            }

        });
    }
}
