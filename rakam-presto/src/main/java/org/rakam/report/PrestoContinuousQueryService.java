package org.rakam.report;

import com.google.inject.Inject;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.util.Tuple;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/04/15 04:38.
 */
public class PrestoContinuousQueryService extends ContinuousQueryService {
    private final QueryMetadataStore database;
    private final PrestoQueryExecutor executor;
    private final Metastore metastore;

    @Inject
    public PrestoContinuousQueryService(QueryMetadataStore database, PrestoQueryExecutor executor, Metastore metastore) {
        super(database);
        this.database = database;
        this.executor = executor;
        this.metastore = metastore;
    }

    @Override
    public CompletableFuture<QueryResult> create(ContinuousQuery report) {
        String query = format("create view stream.%s.%s as (%s)", report.project, report.tableName, report.query);
        return executor.executeRawQuery(query).getResult().thenApply(result -> {
            if(result.getError() == null) {
                database.createContinuousQuery(report);
            }
            return result;
        });
    }

    @Override
    public CompletableFuture<QueryResult> delete(String project, String name) {
        ContinuousQuery continuousQuery = database.getContinuousQuery(project, name);

        String prestoQuery = format("drop view stream.%s.%s", continuousQuery.project, continuousQuery.tableName);
        return executor.executeRawQuery(prestoQuery).getResult().thenApply(result -> {
            if(result.getError() == null) {
                database.createContinuousQuery(continuousQuery);
            }
            return result;
        });
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project) {
        return list(project).stream()
                .map(view -> new Tuple<>(view.tableName, metastore.getCollection(project, view.getTableName())))
                .collect(Collectors.toMap(t -> t.v1(), t -> t.v2()));
    }
}
