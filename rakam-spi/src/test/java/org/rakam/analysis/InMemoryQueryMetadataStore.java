package org.rakam.analysis;

import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;
import org.rakam.util.AlreadyExistsException;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class InMemoryQueryMetadataStore implements QueryMetadataStore {
    private final Set<ContinuousQuery> continuousQueries = new HashSet<>();
    private final Set<MaterializedView> materializedViews = new HashSet<>();

    @Override
    public void createMaterializedView(MaterializedView materializedView) {
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
        return materializedViews.stream()
                .filter(view -> view.project.equals(project) && view.name.equals(name))
                .findAny().get();
    }

    @Override
    public List<MaterializedView> getMaterializedViews(String project) {
        return materializedViews.stream()
                .filter(view -> view.project.equals(project))
                .collect(Collectors.toList());
    }

    @Override
    public boolean updateMaterializedView(MaterializedView view, CompletableFuture<Instant> releaseLock) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createContinuousQuery(ContinuousQuery report) {
        if(continuousQueries.contains(report)) {
            throw new AlreadyExistsException("Continuous query", HttpResponseStatus.BAD_REQUEST);
        }
        continuousQueries.add(report);
    }

    @Override
    public void deleteContinuousQuery(String project, String name) {
        continuousQueries.remove(getContinuousQuery(project, name));
    }

    @Override
    public List<ContinuousQuery> getContinuousQueries(String project) {
        return continuousQueries.stream()
                .filter(report -> report.project.equals(project))
                .collect(Collectors.toList());
    }

    @Override
    public ContinuousQuery getContinuousQuery(String project, String name) {
        return continuousQueries.stream()
                .filter(view -> view.project.equals(project) && view.tableName.equals(name))
                .findAny().get();
    }

    @Override
    public List<ContinuousQuery> getAllContinuousQueries() {
        return ImmutableList.copyOf(continuousQueries);
    }
}
