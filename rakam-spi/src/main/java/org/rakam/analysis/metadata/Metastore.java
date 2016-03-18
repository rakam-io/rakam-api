package org.rakam.analysis.metadata;

import org.rakam.util.NotExistsException;
import org.rakam.collection.SchemaField;

import java.util.List;
import java.util.Map;
import java.util.Set;


public interface Metastore {
    Map<String, Set<String>> getAllCollections();

    Map<String, List<SchemaField>> getCollections(String project);

    Set<String> getCollectionNames(String project);


    void createProject(String project);

    Set<String> getProjects();

    List<SchemaField> getCollection(String project, String collection);

    List<SchemaField> getOrCreateCollectionFieldList(String project, String collection, Set<SchemaField> fields) throws NotExistsException;

    void deleteProject(String project);

    default void setup() {}
}