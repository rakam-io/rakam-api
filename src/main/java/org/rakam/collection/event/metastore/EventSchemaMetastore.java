package org.rakam.collection.event.metastore;

import com.google.common.collect.Table;
import org.apache.avro.Schema;
import org.rakam.collection.SchemaField;

import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/02/15 15:57.
 */
public interface EventSchemaMetastore {
    Table<String, String, Schema> getAllSchemas();

    Map<String, List<String>> getAllCollections();

    Map<String, Schema> getSchemas(String project);

    List<SchemaField> getSchema(String project, String collection);

    List<SchemaField> createOrGetSchema(String project, String collection, List<SchemaField> fields);
}