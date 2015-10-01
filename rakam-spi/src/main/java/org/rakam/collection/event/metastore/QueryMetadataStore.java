package org.rakam.collection.event.metastore;

import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;

import java.time.Instant;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 05:21.
 */
public interface QueryMetadataStore {
    void createMaterializedView(MaterializedView materializedView);

    void deleteMaterializedView(String project, String name);

    MaterializedView getMaterializedView(String project, String name);

    List<MaterializedView> getMaterializedViews(String project);

    List<MaterializedView> getAllMaterializedViews();

    void updateMaterializedView(String project, String name, Instant last_update);

    void createContinuousQuery(ContinuousQuery report);

    void deleteContinuousQuery(String project, String name);

    List<ContinuousQuery> getContinuousQueries(String project);

    ContinuousQuery getContinuousQuery(String project, String tableNme);

    List<ContinuousQuery> getAllContinuousQueries();
}
