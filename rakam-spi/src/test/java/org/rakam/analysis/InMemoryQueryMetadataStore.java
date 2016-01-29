package org.rakam.analysis;

import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class InMemoryQueryMetadataStore implements QueryMetadataStore {
    private final List<ContinuousQuery> continuousQueries = new ArrayList<>();
    private final List<MaterializedView> materializedViews = new ArrayList<>();

    @Override
    public void createMaterializedView(MaterializedView materializedView) {
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
    public boolean updateMaterializedView(MaterializedView view, CompletableFuture<Boolean> releaseLock) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createContinuousQuery(ContinuousQuery report) {
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
        return Collections.unmodifiableList(continuousQueries);
    }
}
