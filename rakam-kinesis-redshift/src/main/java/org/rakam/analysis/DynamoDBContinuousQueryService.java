package org.rakam.analysis;

import com.google.inject.Inject;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.report.QueryResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/07/15 19:25.
 */
public class DynamoDBContinuousQueryService extends ContinuousQueryService {
    @Inject
    public DynamoDBContinuousQueryService(QueryMetadataStore database) {
        super(database);
    }

    @Override
    public CompletableFuture<QueryResult> create(ContinuousQuery report) {
        return null;
    }

    @Override
    public CompletableFuture<QueryResult> delete(String project, String name) {
        return null;
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String s) {
        return null;
    }
}
