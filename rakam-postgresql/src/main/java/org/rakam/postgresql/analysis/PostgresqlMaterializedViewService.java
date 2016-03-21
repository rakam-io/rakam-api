package org.rakam.postgresql.analysis;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Query;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.plugin.MaterializedView;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.util.QueryFormatter;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static java.lang.String.format;

public class PostgresqlMaterializedViewService extends MaterializedViewService {
    private final SqlParser parser = new SqlParser();

    private final PostgresqlQueryExecutor queryExecutor;
    private final QueryMetadataStore database;

    @Inject
    public PostgresqlMaterializedViewService(PostgresqlQueryExecutor queryExecutor, QueryMetadataStore database) {
        super(database, queryExecutor);
        this.queryExecutor = queryExecutor;
        this.database = database;
    }

    @Override
    public CompletableFuture<Void> create(MaterializedView materializedView) {
        materializedView.validateQuery();

        StringBuilder builder = new StringBuilder();
        Query statement;
        synchronized (parser) {
            statement = (Query) parser.createStatement(materializedView.query);
        }

        new QueryFormatter(builder, name -> queryExecutor.formatTableReference(materializedView.project, name)).process(statement, 1);

        QueryResult result = queryExecutor.executeRawStatement(format("CREATE MATERIALIZED VIEW \"%s\".\"%s%s\" AS %s WITH NO DATA",
                materializedView.project, PostgresqlQueryExecutor.MATERIALIZED_VIEW_PREFIX, materializedView.tableName, builder.toString())).getResult().join();
        if (result.isFailed()) {
            throw new RakamException("Couldn't created table: " + result.getError().toString(), UNAUTHORIZED);
        }
        database.createMaterializedView(materializedView);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<QueryResult> delete(String project, String name) {
        MaterializedView materializedView = database.getMaterializedView(project, name);
        database.deleteMaterializedView(project, name);
        return queryExecutor.executeRawStatement(format("DROP MATERIALIZED VIEW \"%s\".\"%s%s\"",
                materializedView.project, PostgresqlQueryExecutor.MATERIALIZED_VIEW_PREFIX, materializedView.tableName)).getResult();
    }

    @Override
    public MaterializedViewExecution lockAndUpdateView(MaterializedView materializedView) {
        CompletableFuture<Instant> f = new CompletableFuture<>();
        boolean availableForUpdating = database.updateMaterializedView(materializedView, f);
        if (availableForUpdating) {
            String reference = String.format("\"%s\".\"%s%s\"", materializedView.project,
                    PostgresqlQueryExecutor.MATERIALIZED_VIEW_PREFIX, materializedView.tableName);

            QueryExecution execution = queryExecutor.executeRawStatement(format("REFRESH MATERIALIZED VIEW " + reference));
            DelegateQueryExecution delegateQueryExecution = new DelegateQueryExecution(execution, result -> {
                f.complete(!result.isFailed() ? Instant.now() : null);
                return result;
            });
            return new MaterializedViewExecution(delegateQueryExecution, reference);
        }
        return null;
    }
}
