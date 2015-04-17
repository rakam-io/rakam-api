package org.rakam.plugin;


import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.report.QueryResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/04/15 04:37.
 */
public abstract class ContinuousQueryService {

    private final QueryMetadataStore database;

    public ContinuousQueryService(QueryMetadataStore database) {
        this.database = database;
    }

    public abstract CompletableFuture<QueryResult> create(ContinuousQuery report);
    public abstract CompletableFuture<QueryResult> delete(String project, String name);

    public List<ContinuousQuery> list(String project) {
        return database.getContinuousQueries(project);
    }

    public ContinuousQuery get(String project, String name) {
        return database.getContinuousQuery(project, name);
    }
}
