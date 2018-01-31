package org.rakam.analysis;

import com.google.common.collect.ImmutableList;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryApiKeyService implements ApiKeyService {
    private final Map<String, List<ProjectApiKeys>> apiKeys = new ConcurrentHashMap<>();

    @Override
    public synchronized ProjectApiKeys createApiKeys(String project) {
        List<ProjectApiKeys> keys = apiKeys.computeIfAbsent(project, p -> new ArrayList<>());

        ProjectApiKeys projectApiKeys = ProjectApiKeys.create(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());
        keys.add(projectApiKeys);
        return projectApiKeys;
    }

    @Override
    public String getProjectOfApiKey(String apiKey, AccessKeyType type) {
        Optional<String> project = apiKeys.entrySet().stream()
                .filter(e -> e.getValue().stream()
                        .anyMatch(a -> a.getKey(type).equals(apiKey)))
                .findAny().map(e -> e.getKey());
        if (!project.isPresent()) {
            throw new IllegalStateException();
        }
        return project.get();
    }

    @Override
    public Key getProjectKey(int apiId, AccessKeyType type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeApiKeys(String project, String masterKey) {
        apiKeys.getOrDefault(project, ImmutableList.of())
                .removeIf(e -> e.masterKey().equals(masterKey));
    }

    private String getKey(ProjectApiKeys keys, AccessKeyType type) {
        switch (type) {
            case MASTER_KEY:
                return keys.masterKey();
            case READ_KEY:
                return keys.readKey();
            case WRITE_KEY:
                return keys.writeKey();
            default:
                throw new IllegalStateException();
        }
    }


    @Override
    public void revokeAllKeys(String project) {
        apiKeys.remove(project);
    }
}
