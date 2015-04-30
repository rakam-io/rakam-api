package org.rakam.collection.event.metastore;

import org.rakam.collection.SchemaField;

import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/02/15 15:57.
 */
public interface Metastore {
    Map<String, List<String>> getAllCollections();
    Map<String, List<SchemaField>> getCollections(String project);
    List<SchemaField> getCollection(String project, String collection);
    List<SchemaField> createOrGetCollectionField(String project, String collection, List<SchemaField> fields);
}