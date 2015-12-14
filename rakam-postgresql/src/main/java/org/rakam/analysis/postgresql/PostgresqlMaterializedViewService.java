package org.rakam.analysis.postgresql;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Query;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.MaterializedView;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;
import org.rakam.util.QueryFormatter;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.time.Clock;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static java.lang.String.format;

public class PostgresqlMaterializedViewService extends MaterializedViewService {
    private final PostgresqlMetastore metastore;
    private final SqlParser parser = new SqlParser();

    private final PostgresqlQueryExecutor queryExecutor;

    @Inject
    public PostgresqlMaterializedViewService(PostgresqlQueryExecutor queryExecutor, QueryMetadataStore database, PostgresqlMetastore metastore, Clock clock) {
        super(queryExecutor, database, clock);
        this.metastore = metastore;
        this.queryExecutor = queryExecutor;
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
        if(result.isFailed()) {
            throw new RakamException("Couldn't created table: "+result.getError().toString(), UNAUTHORIZED);
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
    public QueryExecution lockAndUpdateView(MaterializedView materializedView) {
        CompletableFuture<Boolean> f = new CompletableFuture<>();
        boolean availableForUpdating = database.updateMaterializedView(materializedView, f);
        if(availableForUpdating) {
            QueryExecution execution = queryExecutor.executeRawStatement(format("REFRESH MATERIALIZED VIEW \"%s\".\"%s%s\"", materializedView.project,
                    PostgresqlQueryExecutor.MATERIALIZED_VIEW_PREFIX, materializedView.tableName));
            return new DelegateQueryExecution(execution, result -> {
                f.complete(!result.isFailed());
                return result;
            });
        }
        return null;
    }

    public Map<String, List<SchemaField>> getSchemas(String project) {
        return list(project).stream()
                .map(view -> new SimpleImmutableEntry<>(view.tableName, metastore.getCollection(project, queryExecutor.MATERIALIZED_VIEW_PREFIX + view.tableName)))
                .collect(Collectors.toMap(t -> t.getKey(), t -> t.getValue()));
    }
}
