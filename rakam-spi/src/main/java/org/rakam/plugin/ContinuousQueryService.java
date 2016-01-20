package org.rakam.plugin;

import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.report.QueryResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class ContinuousQueryService {

    protected final QueryMetadataStore database;

    public ContinuousQueryService(QueryMetadataStore database) {
        this.database = database;
    }

    public abstract CompletableFuture<QueryResult> create(ContinuousQuery report);

    public abstract CompletableFuture<Boolean> delete(String project, String name);

    public List<ContinuousQuery> list(String project) {
        return database.getContinuousQueries(project);
    }

    public ContinuousQuery get(String project, String name) {
        return database.getContinuousQuery(project, name);
    }

    public abstract Map<String, List<SchemaField>> getSchemas(String project);

    public abstract boolean test(String project, String query);
}
