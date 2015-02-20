package org.rakam.collection.event.metastore;

import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.avro.Schema;
import org.rakam.kume.Cluster;
import org.rakam.kume.service.ringmap.RingMap;
import org.rakam.util.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by buremba <Burak Emre Kabakcı> on 03/02/15 14:23.
 */
@Singleton
public class KumeSchemaMetastore implements EventSchemaMetastore {

    private final RingMap<String, Schema> registries;

    @Inject
    public KumeSchemaMetastore(Cluster cluster) {
        this.registries = cluster.createOrGetService("avroSchemaRegistry", bus -> new RingMap<>(bus, (first, second) -> first, 2));
    }

    @Override
    public Table<String, String, Schema> getAllSchemas() {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, List<String>> getAllCollections() {
        return null;
    }

    @Override
    public Map<String, Schema> getSchemas(String project) {
        throw new NotImplementedException();
    }

    @Override
    public Schema getSchema(String project, String collection) {
        return registries.get(project+"_"+collection).join();
    }

    @Override
    public Schema createOrGetSchema(String project, String collection,  List<Schema.Field> newFields) {
        Schema record = Schema.createRecord(newFields);
        return registries.merge(project+"_"+collection, record, (oldSchema, newSchema) -> {
            ArrayList<Schema.Field> fields = Lists.newArrayList(newSchema.getFields());
            for (Schema.Field field : fields) {
                Optional<Schema.Field> first = fields.stream().filter(x -> x.name().equals(field.name())).findFirst();
                if(!first.isPresent()) {
                    fields.add(field);
                }
            }

            Schema schema = Schema.createRecord("rakam", null, "org.rakam.model", false);
            schema.setFields(fields);
            return schema;
        }).join();
    }
}
