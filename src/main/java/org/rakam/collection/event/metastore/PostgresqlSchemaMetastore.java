package org.rakam.collection.event.metastore;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.avro.Schema;
import org.rakam.report.metadata.postgresql.PostgresqlConfig;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static org.rakam.collection.event.EventDeserializer.copyFields;

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
                "  schema TEXT NOT NULL,\n" +
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
    public Map<String, String> getAllCollections() {
        Query<Map<String, Object>> bind = dao.createQuery("SELECT project, collection from collection_schema");

        Map<String, String> table = Maps.newHashMap();
        bind.forEach(row ->
                table.put((String) row.get("project"), (String) row.get("collection")));

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
    public Schema getSchema(String project, String collection) {
        return getSchema(dao, project, collection);
    }

    private Schema getSchema(Handle dao, String project, String collection) {
        Query<Map<String, Object>> bind = dao.createQuery("SELECT schema from collection_schema WHERE project = :project AND collection = :collection")
                .bind("project", project)
                .bind("collection", collection);
        Map<String, Object> o = bind.first();
        return o != null ? new Schema.Parser().parse((String) o.get("schema")) : null;
    }

    @Override
    public Schema createOrGetSchema(String project, String collection, List<Schema.Field> newFields) {
        return dbi.inTransaction((dao, status) -> {
            Schema o = getSchema(dao, project, collection);
            if(o == null) {
                Schema record = Schema.createRecord(collection, null, project, false);
                record.setFields(newFields);
                dao.createStatement("INSERT INTO collection_schema (project, collection, schema) VALUES (:project, :collection, :schema)")
                        .bind("schema", record.toString())
                        .bind("project", project)
                        .bind("collection", collection)
                        .execute();
                return record;
            } else {

                List<Schema.Field> fields = copyFields(o.getFields());
                for (Schema.Field field : newFields) {
                    Optional<Schema.Field> first = fields.stream().filter(x -> x.name().equals(field.name())).findFirst();
                    if(!first.isPresent()) {
                        fields.add(field);
                    } else {
                        if(!first.get().schema().equals(field.schema()))
                            throw new IllegalArgumentException();
                    }
                }

                Schema record = Schema.createRecord(o.getName(), o.getDoc(), o.getNamespace(), false);
                record.setFields(fields);
                dao.createStatement("UPDATE collection_schema SET schema = :schema WHERE project = :project AND collection = :collection")
                        .bind("schema", record.toString())
                        .bind("project", project)
                        .bind("collection", collection)
                        .execute();
                return record;
            }

        });
    }
}
