package org.rakam.analysis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.metadata.AbstractMetastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.FieldDependencyBuilder.FieldDependency;
import org.rakam.collection.SchemaField;
import org.rakam.util.NotExistsException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class InMemoryMetastore extends AbstractMetastore {
    private final Map<String, Map<String, List<SchemaField>>> collections = new ConcurrentHashMap<>();
    private final ApiKeyService apiKeyService;

    public InMemoryMetastore(ApiKeyService apiKeyService) {
        super(new EventBus());
        this.apiKeyService = apiKeyService;
    }

    public InMemoryMetastore(ApiKeyService apiKeyService, EventBus eventBus) {
        super(eventBus);
        this.apiKeyService = apiKeyService;
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
    public void createProject(String project) {
        collections.put(project, new HashMap<>());
    }

    @Override
    public Set<String> getProjects() {
        return collections.keySet();
    }

    @Override
    public List<SchemaField> getCollection(String project, String collection) {
        return collections.getOrDefault(project, ImmutableMap.of()).getOrDefault(collection, ImmutableList.of());
    }

    @Override
    public synchronized List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields) throws NotExistsException {
        Map<String, List<SchemaField>> list = collections.get(project);
        if(list == null) {
            throw new NotExistsException("Project");
        }
        List<SchemaField> schemaFields = list.computeIfAbsent(collection, (key) -> new ArrayList<>());
        fields.stream()
                .filter(field -> !schemaFields.stream().anyMatch(f -> f.getName().equals(field.getName())))
                .forEach(schemaFields::add);
        return schemaFields;
    }

    @Override
    public void deleteProject(String project) {
        collections.remove(project);
        apiKeyService.revokeAllKeys(project);
        super.onDeleteProject(project);
    }
}
