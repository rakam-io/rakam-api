package org.rakam.aws;

import org.rakam.analysis.ProjectNotExistsException;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class InMemoryMetastore implements Metastore {
    private final Map<String, Map<String, List<SchemaField>>> collections = new HashMap<>();

    @Override
    public Map<String, Set<String>> getAllCollections() {
        return Collections.unmodifiableMap(collections.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey(),
                        entry -> entry.getValue().keySet())));
    }

    @Override
    public Map<String, List<SchemaField>> getCollections(String project) {
        return collections.get(project);
    }

    @Override
    public Set<String> getCollectionNames(String project) {
        return collections.get(project).keySet();
    }

    @Override
    public ProjectApiKeyList createApiKeys(String project) {
        return null;
    }

    @Override
    public void createProject(String project) {
        collections.put(project, new HashMap<>());
    }

    @Override
    public Set<String> getProjects() {
        return collections.keySet();
    }

    @Override
    public List<SchemaField> getCollection(String project, String collection) {
        return collections.get(project).get(collection);
    }

    @Override
    public synchronized List<SchemaField> createOrGetCollectionField(String project, String collection, List<SchemaField> fields) throws ProjectNotExistsException {
        Map<String, List<SchemaField>> list = collections.get(project);
        if(list == null) {
            throw new ProjectNotExistsException();
        }
        List<SchemaField> schemaFields = list.computeIfAbsent(collection, (key) -> new ArrayList<>());
        fields.stream()
                .filter(field -> !schemaFields.contains(field))
                .forEach(schemaFields::add);
        return schemaFields;
    }

    @Override
    public boolean checkPermission(String project, AccessKeyType type, String apiKey) {
        return false;
    }

}
