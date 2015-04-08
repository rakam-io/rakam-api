package org.rakam.report;

import com.google.inject.Inject;
import org.rakam.collection.event.metastore.ReportMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;

import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/04/15 04:38.
 */
public class PrestoContinuousQueryService extends ContinuousQueryService {
    private final ReportMetadataStore database;
    private final QueryExecutor executor;

    @Inject
    public PrestoContinuousQueryService(ReportMetadataStore database, QueryExecutor executor) {
        super(database);
        this.database = database;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<QueryResult> create(ContinuousQuery report) {
        String query = format("create view stream.%s.%s as (%s)", report.project, report.tableName, report.query);
        return executor.executeQuery(query).getResult().thenApply(result -> {
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
        return executor.executeQuery(prestoQuery).getResult().thenApply(result -> {
            if(result.getError() == null) {
                database.createContinuousQuery(continuousQuery);
            }
            return result;
        });
    }
}
