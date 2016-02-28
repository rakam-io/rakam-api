package org.rakam.analysis;

import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.metadata.AbstractMetastore;
import org.rakam.collection.SchemaField;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.util.NotExistsException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class InMemoryMetastore extends AbstractMetastore {
    private final Map<String, Map<String, List<SchemaField>>> collections = new ConcurrentHashMap<>();
    private final Map<String, List<ProjectApiKeys>> apiKeys = new ConcurrentHashMap<>();
    private final AtomicInteger apiKeyCounter = new AtomicInteger();

    public InMemoryMetastore() {
        super(new FieldDependencyBuilder().build(), new EventBus());
    }

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
    public synchronized ProjectApiKeys createApiKeys(String project) {
        List<ProjectApiKeys> keys = apiKeys.computeIfAbsent(project, p -> new ArrayList<>());

        ProjectApiKeys projectApiKeys = new ProjectApiKeys(apiKeyCounter.incrementAndGet(), project,
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());
        keys.add(projectApiKeys);
        return projectApiKeys;
    }

    @Override
    public synchronized void revokeApiKeys(String project, int id) {
        Iterator<ProjectApiKeys> keys = apiKeys.getOrDefault(project, ImmutableList.of()).iterator();
        while(keys.hasNext()) {
            if(keys.next().id == id) {
                keys.remove();
            }
        }
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
        return collections.get(project).getOrDefault(collection, ImmutableList.of());
    }

    @Override
    public synchronized List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields) throws NotExistsException {
        Map<String, List<SchemaField>> list = collections.get(project);
        if(list == null) {
            throw new NotExistsException("project", HttpResponseStatus.UNAUTHORIZED);
        }
        List<SchemaField> schemaFields = list.computeIfAbsent(collection, (key) -> new ArrayList<>());
        fields.stream()
                .filter(field -> !schemaFields.stream().anyMatch(f -> f.getName().equals(field.getName())))
                .forEach(schemaFields::add);
        return schemaFields;
    }

    @Override
    public boolean checkPermission(String project, AccessKeyType type, String apiKey) {
        return apiKeys.getOrDefault(project, ImmutableList.of()).stream().anyMatch(a -> apiKey.equals(getKey(a, type)));
    }

    private String getKey(ProjectApiKeys keys, AccessKeyType type) {
        switch (type) {
            case MASTER_KEY:
                return keys.masterKey;
            case READ_KEY:
                return keys.readKey;
            case WRITE_KEY:
                return keys.writeKey;
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public List<ProjectApiKeys> getApiKeys(int[] ids) {
        return apiKeys.entrySet().stream().flatMap(a -> a.getValue().stream())
                .filter(a -> Arrays.asList(ids).contains(a.id))
                .collect(Collectors.toList());
    }

    @Override
    public void deleteProject(String project) {
        collections.remove(project);
        apiKeys.remove(project);
        super.onDeleteProject(project);
    }
}
