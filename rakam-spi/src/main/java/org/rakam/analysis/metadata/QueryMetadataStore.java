package org.rakam.analysis.metadata;

import org.rakam.plugin.MaterializedView;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;


public interface QueryMetadataStore {
    void createMaterializedView(String project, MaterializedView materializedView);

    void deleteMaterializedView(String project, String tableName);

    MaterializedView getMaterializedView(String project, String tableName);

    List<MaterializedView> getMaterializedViews(String project);

    boolean updateMaterializedView(String project, MaterializedView view, CompletableFuture<Instant> releaseLock);

    void changeMaterializedView(String project, String tableName, boolean realTime);

    void alter(String project, MaterializedView view);
}
