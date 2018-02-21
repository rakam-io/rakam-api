package org.rakam.analysis;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.plugin.MaterializedView;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.MaterializedViewNotExists;
import org.rakam.util.NotExistsException;
import org.rakam.util.ProjectCollection;

import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class InMemoryQueryMetadataStore
        implements QueryMetadataStore {
    private final Map<String, Set<MaterializedView>> materializedViews = new HashMap<>();
    private final Set<ProjectCollection> locks = new HashSet<>();

    @Override
    public void createMaterializedView(String project, MaterializedView materializedView) {
        Set<MaterializedView> materializedViews = this.materializedViews.computeIfAbsent(project, (key) -> new HashSet<>());

        if (materializedViews.contains(materializedView)) {
            throw new AlreadyExistsException("Materialized view", HttpResponseStatus.BAD_REQUEST);
        }
        materializedViews.add(materializedView);
    }

    @Override
    public void deleteMaterializedView(String project, String name) {
        Set<MaterializedView> materializedViews = this.materializedViews.get(project);
        if (materializedViews == null) {
            throw new IllegalArgumentException();
        }
        materializedViews.remove(getMaterializedView(project, name));
    }

    @Override
    public MaterializedView getMaterializedView(String project, String name) {
        return materializedViews.computeIfAbsent(project, (key) -> new HashSet<>()).stream()
                .filter(view -> view.tableName.equals(name))
                .findAny().orElseThrow(() -> new MaterializedViewNotExists(name));
    }

    @Override
    public List<MaterializedView> getMaterializedViews(String project) {
        return materializedViews.computeIfAbsent(project, (key) -> new HashSet<>()).stream()
                .filter(view -> project.equals(project))
                .collect(Collectors.toList());
    }

    @Override
    public synchronized boolean updateMaterializedView(String project, MaterializedView userView, CompletableFuture<Instant> releaseLock) {
        MaterializedView view = materializedViews.get(project).stream()
                .filter(e -> e.tableName.equals(userView.tableName)).findFirst().get();

        if (!view.needsUpdate(Clock.systemUTC())) {
            return false;
        }

        ProjectCollection table = new ProjectCollection(project, userView.tableName);

        boolean notLocked = locks.add(table);

        if (notLocked) {
            releaseLock.whenComplete((success, ex) -> {
                if (success != null) {
                    locks.remove(table);
                    view.lastUpdate = success;
                }
            });
        }

        return notLocked;
    }

    @Override
    public synchronized void changeMaterializedView(String project, String tableName, boolean realTime) {
        Set<MaterializedView> materializedViews = this.materializedViews.get(project);
        MaterializedView materializedView = materializedViews.stream().filter(e -> e.tableName.equals(tableName))
                .findAny().orElseThrow(() -> new NotExistsException("Materialized view"));
        materializedViews.remove(materializedView);
        materializedViews.add(new MaterializedView(materializedView.tableName,
                materializedView.name, materializedView.query, materializedView.updateInterval, materializedView.incremental,
                realTime, materializedView.options));
    }

    @Override
    public void alter(String project, MaterializedView view) {
        Set<MaterializedView> materializedViews = this.materializedViews.computeIfAbsent(project, (key) -> new HashSet<>());

        if (!materializedViews.contains(view)) {
            throw new NotExistsException("Materialized view");
        }
        materializedViews.add(view);
    }
}
