package org.rakam.analysis;

import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;
import org.rakam.util.AlreadyExistsException;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class InMemoryQueryMetadataStore implements QueryMetadataStore {
    private final Map<String, Set<ContinuousQuery>> continuousQueries = new HashMap<>();
    private final Map<String, Set<MaterializedView>> materializedViews = new HashMap<>();

    @Override
    public void createMaterializedView(String project, MaterializedView materializedView) {
        Set<MaterializedView> materializedViews = this.materializedViews.computeIfAbsent(project, (key) -> new HashSet<>());

        if(materializedViews.contains(materializedView)) {
            throw new AlreadyExistsException("Materialized view", HttpResponseStatus.BAD_REQUEST);
        }
        materializedViews.add(materializedView);
    }

    @Override
    public void deleteMaterializedView(String project, String name) {
        materializedViews.remove(getMaterializedView(project, name));
    }

    @Override
    public MaterializedView getMaterializedView(String project, String name) {
        return materializedViews.computeIfAbsent(project, (key) -> new HashSet<>()).stream()
                .filter(view -> view.tableName.equals(name))
                .findAny().get();
    }

    @Override
    public List<MaterializedView> getMaterializedViews(String project) {
        return materializedViews.computeIfAbsent(project, (key) -> new HashSet<>()).stream()
                .filter(view -> project.equals(project))
                .collect(Collectors.toList());
    }

    @Override
    public boolean updateMaterializedView(String project, MaterializedView view, CompletableFuture<Instant> releaseLock) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createContinuousQuery(String project, ContinuousQuery report) {
        Set<ContinuousQuery> continuousQueries = this.continuousQueries.computeIfAbsent(project, (key) -> new HashSet<>());
        if(continuousQueries.contains(report)) {
            throw new AlreadyExistsException("Continuous query", HttpResponseStatus.BAD_REQUEST);
        }
        continuousQueries.add(report);
    }

    @Override
    public void deleteContinuousQuery(String project, String name) {
        continuousQueries.computeIfAbsent(project, (key) -> new HashSet<>()).remove(getContinuousQuery(project, name));
    }

    @Override
    public List<ContinuousQuery> getContinuousQueries(String project) {
        return continuousQueries.computeIfAbsent(project, (key) -> new HashSet<>()).stream()
                .filter(report -> project.equals(project))
                .collect(Collectors.toList());
    }

    @Override
    public ContinuousQuery getContinuousQuery(String project, String name) {
        return continuousQueries.computeIfAbsent(project, (key) -> new HashSet<>()).stream()
                .filter(view -> project.equals(project) && view.tableName.equals(name))
                .findAny().get();
    }
}
