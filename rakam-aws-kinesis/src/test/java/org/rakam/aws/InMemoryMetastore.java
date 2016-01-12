package org.rakam.aws;

import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.AbstractMetastore;
import org.rakam.analysis.NotExistsException;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.FieldDependencyBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class InMemoryMetastore extends AbstractMetastore {
    private final Map<String, Map<String, List<SchemaField>>> collections = new HashMap<>();

    @Inject
    public InMemoryMetastore(FieldDependencyBuilder.FieldDependency fieldDependency, EventBus eventBus) {
        super(fieldDependency, eventBus);
    }

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
    public ProjectApiKeys createApiKeys(String project) {
        return null;
    }

    @Override
    public void revokeApiKeys(String project, int keys) {

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
    public synchronized List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields) throws NotExistsException {
        Map<String, List<SchemaField>> list = collections.get(project);
        if(list == null) {
            throw new NotExistsException("project", HttpResponseStatus.UNAUTHORIZED);
        }
        List<SchemaField> schemaFields = list.computeIfAbsent(collection, (key) -> new ArrayList<SchemaField>());
        fields.stream()
                .filter(field -> !schemaFields.contains(field))
                .forEach(schemaFields::add);
        return schemaFields;
    }

    @Override
    public boolean checkPermission(String project, AccessKeyType type, String apiKey) {
        return false;
    }

    @Override
    public List<ProjectApiKeys> getApiKeys(int[] ids) {
        return null;
    }

    @Override
    public void deleteProject(String project) {
        collections.remove(project);
    }
}
