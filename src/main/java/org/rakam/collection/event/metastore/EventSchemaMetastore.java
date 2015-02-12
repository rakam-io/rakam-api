package org.rakam.collection.event.metastore;

import com.google.common.collect.Table;
import org.apache.avro.Schema;

import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/02/15 15:57.
 */
public interface EventSchemaMetastore {
    Table<String, String, Schema> getAllSchemas();

    Map<String, Schema> getSchemas(String project);

    Schema getSchema(String project, String collection);

    Schema createOrGetSchema(String project, String collection, List<Schema.Field> fields);
}