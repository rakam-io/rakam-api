package org.rakam.collection.event.metastore;

import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;

import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 05:21.
 */
public interface ReportMetadataStore {
    public void saveMaterializedView(MaterializedView materializedView);

    public void deleteMaterializedView(String project, String name);

    public MaterializedView getMaterializedView(String project, String name);

    public List<MaterializedView> getMaterializedViews(String project);

    public void createContinuousQuery(ContinuousQuery report);

    public void deleteContinuousQuery(String project, String name);

    public List<ContinuousQuery> getContinuousQueries(String project);

    public ContinuousQuery getContinuousQuery(String project, String name);

    Map<String, List<ContinuousQuery>> getAllContinuousQueries();
}
