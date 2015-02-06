package org.rakam.collection.event;

import com.facebook.presto.jdbc.internal.guava.collect.Lists;
import com.google.inject.Inject;
import org.apache.avro.Schema;
import org.rakam.kume.Cluster;
import org.rakam.kume.service.ringmap.RingMap;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 03/02/15 14:23.
 */
public class AvroSchemaRegistryService {

    private final RingMap<String, Schema> registries;

    @Inject
    public AvroSchemaRegistryService(Cluster cluster) {
        this.registries = cluster.createOrGetService("avroSchemaRegistry", bus -> new RingMap<>(bus, (first, second) -> first, 2));
    }

    public Schema getSchema(String projectCollection) {
        return registries.get(projectCollection).join();
    }

    @Nonnull
    public Schema addFieldsAndGetSchema(String key, List<Schema.Field> newFields) {
        Schema record = Schema.createRecord(newFields);
        return registries.merge(key, record, (oldSchema, newSchema) -> {
            ArrayList<Schema.Field> fields = Lists.newArrayList(newSchema.getFields());
            fields.addAll(newFields);

            Schema schema = Schema.createRecord("rakam", null, "avro.test", false);
            schema.setFields(fields);
            return schema;
        }).join();
    }
}
