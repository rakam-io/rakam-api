package org.rakam.collection.event.metastore;

import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;

import java.util.List;
import java.util.concurrent.CompletableFuture;


public interface QueryMetadataStore {
    void createMaterializedView(MaterializedView materializedView);

    void deleteMaterializedView(String project, String name);

    MaterializedView getMaterializedView(String project, String tableName);

    List<MaterializedView> getMaterializedViews(String project);

    boolean updateMaterializedView(MaterializedView view, CompletableFuture<Boolean> releaseLock);

    void createContinuousQuery(ContinuousQuery report);

    void deleteContinuousQuery(String project, String name);

    List<ContinuousQuery> getContinuousQueries(String project);

    ContinuousQuery getContinuousQuery(String project, String tableNme);

    List<ContinuousQuery> getAllContinuousQueries();
}
