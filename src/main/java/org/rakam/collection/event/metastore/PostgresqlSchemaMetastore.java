package org.rakam.collection.event.metastore;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.apache.avro.Schema;
import org.rakam.report.metadata.postgresql.PostgresqlMetadataConfig;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/02/15 15:57.
 */
public class PostgresqlSchemaMetastore implements EventSchemaMetastore {

    private final Handle dao;
    private final DBI dbi;

    public PostgresqlSchemaMetastore(PostgresqlMetadataConfig config) {
        dbi = new DBI(format("jdbc:postgresql://%s/%s", config.getHost(), config.getDatabase()),
                config.getUsername(), config.getUsername());
        dao = dbi.open();

        dao.createStatement("CREATE TABLE IF NOT EXISTS collection_schema (" +
                "  project VARCHAR(255) NOT NULL,\n" +
                "  collection VARCHAR(255) NOT NULL,\n" +
                "  schema TEXT NOT NULL)")
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
    public Map<String, Schema> getSchemas(String project) {
        Query<Map<String, Object>> bind = dao.createQuery("SELECT schema from collection_schema WHERE project = :project")
                .bind("project", project);

        HashMap<String, Schema> table = Maps.newHashMap();
        bind.forEach(row ->
                table.put((String) row.get("collection"), (Schema) row.get("schema")));
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
    public Schema createOrGetSchema(String project, String collection, List<Schema.Field> fields) {
        return dbi.inTransaction((dao, status) -> {
            Schema o = getSchema(dao, project, collection);
            if(o == null) {
                Schema record = Schema.createRecord(collection, null, project, false);
                record.setFields(fields);
                dao.createStatement("INSERT INTO collection_schema (project, collection, schema) VALUES (:project, :collection, :schema)")
                        .bind("schema", record.toString())
                        .bind("project", project)
                        .bind("collection", collection)
                        .execute();
                return record;
            } else {
                List<Schema.Field> newFields = o.getFields().stream()
                        .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()))
                        .collect(Collectors.toList());
                for (Schema.Field field : fields) {
                    Optional<Schema.Field> first = newFields.stream().filter(x -> x.name().equals(field.name())).findFirst();
                    if(!first.isPresent()) {
                        newFields.add(field);
                    }
                }

                Schema record = Schema.createRecord(collection, null, project, false);
                record.setFields(newFields);
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
