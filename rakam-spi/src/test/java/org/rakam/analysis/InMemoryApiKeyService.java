package org.rakam.analysis;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class InMemoryApiKeyService implements ApiKeyService {
    private final Map<String, List<ProjectApiKeys>> apiKeys = new ConcurrentHashMap<>();
    private final AtomicInteger apiKeyCounter = new AtomicInteger();

    @Override
    public synchronized ProjectApiKeys createApiKeys(String project) {
        List<ProjectApiKeys> keys = apiKeys.computeIfAbsent(project, p -> new ArrayList<>());

        ProjectApiKeys projectApiKeys = new ProjectApiKeys(apiKeyCounter.incrementAndGet(), project,
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
        if(!project.isPresent()) {
            throw new IllegalStateException();
        }
        return project.get();
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
    public void revokeAllKeys(String project) {
        apiKeys.remove(project);
    }
}
