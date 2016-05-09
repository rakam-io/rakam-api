package org.rakam.analysis.metadata;

import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


public interface QueryMetadataStore {
    void createMaterializedView(String project, MaterializedView materializedView);

    void deleteMaterializedView(String project, String name);

    MaterializedView getMaterializedView(String project, String tableName);

    List<MaterializedView> getMaterializedViews(String project);

    boolean updateMaterializedView(String project, MaterializedView view, CompletableFuture<Instant> releaseLock);

    void createContinuousQuery(String project, ContinuousQuery report);

    void deleteContinuousQuery(String project, String name);

    List<ContinuousQuery> getContinuousQueries(String project);

    ContinuousQuery getContinuousQuery(String project, String tableNme);

    Map<String, Collection<ContinuousQuery>> getAllContinuousQueries();
}
