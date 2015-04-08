package org.rakam.plugin;


import org.rakam.collection.event.metastore.ReportMetadataStore;
import org.rakam.report.QueryResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/04/15 04:37.
 */
public abstract class ContinuousQueryService {

    private final ReportMetadataStore database;

    public ContinuousQueryService(ReportMetadataStore database) {
        this.database = database;
    }

    public abstract CompletableFuture<QueryResult> create(ContinuousQuery report);
    public abstract CompletableFuture<QueryResult> delete(String project, String name);

    public List<ContinuousQuery> list(String project) {
        return database.getContinuousQueries(project);
    }

}
