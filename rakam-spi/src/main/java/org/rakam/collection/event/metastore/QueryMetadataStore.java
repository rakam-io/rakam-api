package org.rakam.collection.event.metastore;

import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;

import java.time.Instant;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 05:21.
 */
public interface QueryMetadataStore {
    public void saveMaterializedView(MaterializedView materializedView);

    public void deleteMaterializedView(String project, String name);

    public MaterializedView getMaterializedView(String project, String name);

    public List<MaterializedView> getMaterializedViews(String project);

    public List<MaterializedView> getAllMaterializedViews();

    void updateMaterializedView(String project, String name, Instant last_update);

    public void createContinuousQuery(ContinuousQuery report);

    public void deleteContinuousQuery(String project, String name);

    public List<ContinuousQuery> getContinuousQueries(String project);

    public ContinuousQuery getContinuousQuery(String project, String name);

    List<ContinuousQuery> getAllContinuousQueries();
}
