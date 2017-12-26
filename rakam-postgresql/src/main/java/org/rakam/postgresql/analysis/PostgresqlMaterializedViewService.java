package org.rakam.postgresql.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.datasource.CustomDataSource;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.plugin.MaterializedView;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.util.*;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.sql.RakamSqlFormatter.formatSql;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static org.rakam.postgresql.report.PostgresqlQueryExecutor.MATERIALIZED_VIEW_PREFIX;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkProject;

public class PostgresqlMaterializedViewService extends MaterializedViewService {
    private final PostgresqlQueryExecutor queryExecutor;
    private final QueryMetadataStore database;
    private final Clock clock;

    @Inject
    public PostgresqlMaterializedViewService(PostgresqlQueryExecutor queryExecutor, QueryMetadataStore database, Clock clock) {
        super(database, queryExecutor, '"');
        this.queryExecutor = queryExecutor;
        this.database = database;
        this.clock = clock;
    }

    @Override
    public CompletableFuture<Void> create(String project, MaterializedView materializedView) {
        String format;
        try {
            materializedView.validateQuery();

            StringBuilder builder = new StringBuilder();
            Query statement = (Query) SqlUtil.parseSql(materializedView.query);

            new RakamSqlFormatter.Formatter(builder, name -> queryExecutor
                    .formatTableReference(project, name, Optional.empty(), new HashMap<String, String>() {
                        @Override
                        public String put(String key, String value)
                        {
                            CustomDataSource read = JsonHelper.read(value, CustomDataSource.class);
                            if(!ImmutableList.of("postgresql", "mysql").contains(read.type)) {
                                throw new RakamException("Cross database materialized views are not supported in Postgresql deployment type.", BAD_REQUEST);
                            }

                            throw new RakamException("Cross database materialized views are not supported in Postgresql deployment type.", BAD_REQUEST);
                        }
                    }), '"').process(statement, 1);

            if(!materializedView.incremental) {
                format = format("CREATE MATERIALIZED VIEW %s.%s AS %s WITH NO DATA",
                        checkProject(project, '"'), checkCollection(MATERIALIZED_VIEW_PREFIX + materializedView.tableName), builder.toString());
            } else {
                format = format("CREATE TABLE %s.%s AS %s WITH NO DATA",
                        checkProject(project, '"'), checkCollection(MATERIALIZED_VIEW_PREFIX + materializedView.tableName), builder.toString());
            }
        }
        catch (Exception e) {
            CompletableFuture<Void> f = new CompletableFuture<>();
            f.completeExceptionally(e);
            return f;
        }

        return queryExecutor.executeRawStatement(format).getResult().thenAccept(result -> {
            if (result.isFailed()) {
                try {
                    get(project, materializedView.tableName);
                    throw new AlreadyExistsException("Materialized view", BAD_REQUEST);
                }
                catch (NotExistsException e) {
                }

                if(!"42P07".equals(result.getError().sqlState)) {
                    throw new RakamException("Couldn't created table: " +
                            result.getError().toString(), BAD_REQUEST);
                }
            }

            database.createMaterializedView(project, materializedView);
        });
    }

    @Override
    public CompletableFuture<QueryResult> delete(String project, String name) {
        MaterializedView materializedView = database.getMaterializedView(project, name);

        String type = materializedView.incremental ? "TABLE" : "MATERIALIZED VIEW";
        QueryExecution queryExecution = queryExecutor.executeRawStatement(format("DROP %s %s.%s",
                type,
                checkProject(project, '"'), checkCollection(MATERIALIZED_VIEW_PREFIX + materializedView.tableName)));
        return queryExecution.getResult().thenApply(result -> {
            if(!result.isFailed())
                database.deleteMaterializedView(project, name);
            else
                return result;

            return QueryResult.empty();
        });
    }

    @Override
    public MaterializedViewExecution lockAndUpdateView(String project, MaterializedView materializedView) {
        CompletableFuture<Instant> f = new CompletableFuture<>();

        String tableName = queryExecutor.formatTableReference(project,
                QualifiedName.of("materialized", materializedView.tableName), Optional.empty(), ImmutableMap.of());
        Query statement = (Query) SqlUtil.parseSql(materializedView.query);

        Map<String, String> sessionProperties = new HashMap<>();
        if (!materializedView.incremental) {
            if (!materializedView.needsUpdate(clock) || !database.updateMaterializedView(project, materializedView, f)) {
                return new MaterializedViewExecution(null, tableName);
            }

            String collection = checkCollection( MATERIALIZED_VIEW_PREFIX + materializedView.tableName);
            QueryExecution execution = queryExecutor.executeRawStatement(format("REFRESH MATERIALIZED VIEW %s.%s ",
                    project, collection));
            DelegateQueryExecution delegateQueryExecution = new DelegateQueryExecution(execution, result -> {
                f.complete(!result.isFailed() ? Instant.now() : null);
                return result;
            });
            return new MaterializedViewExecution(delegateQueryExecution, tableName);
        }
        else {
            String materializedTableReference = tableName;

            boolean willBeUpdated = materializedView.needsUpdate(clock) && database.updateMaterializedView(project, materializedView, f);

            QueryExecution queryExecution;
            Instant now = Instant.now();
            if (willBeUpdated) {
                String query = formatSql(statement,
                        name -> {
                            String predicate =  materializedView.lastUpdate != null ? format("between timezone('UTC', to_timestamp(%d)) and  timezone('UTC', to_timestamp(%d))",
                                    materializedView.lastUpdate.getEpochSecond(), now.getEpochSecond()) :
                                    format(" < timezone('UTC', to_timestamp(%d))", now.getEpochSecond());

                            String collection = queryExecutor.formatTableReference(project, name, Optional.empty(),
                                    ImmutableMap.of());
                            return format("(SELECT * FROM %s WHERE \"$server_time\" %s) data", collection, predicate);
                        }, '"');

                queryExecution = queryExecutor.executeRawStatement(format("INSERT INTO %s %s", materializedTableReference, query), sessionProperties);
                queryExecution.getResult().thenAccept(result -> f.complete(!result.isFailed() ? now : null));

            }
            else {
                queryExecution = QueryExecution.completedQueryExecution("", QueryResult.empty());
                f.complete(materializedView.lastUpdate);
            }

            String reference;
            if (!willBeUpdated && !materializedView.realTime) {
                reference = materializedTableReference;
            }
            else {
                String query = formatSql(statement,
                        name -> {
                            String collection = format("(SELECT * FROM %s %s) data",
                                    queryExecutor.formatTableReference(project, name, Optional.empty(), ImmutableMap.of()),
                                    format("WHERE \"$server_time\" > to_timestamp(%d)",
                                            ( materializedView.lastUpdate != null ?  materializedView.lastUpdate : now).getEpochSecond()));
                            return collection;
                        }, '"');

                reference = format("(SELECT * from %s UNION ALL %s) data", materializedTableReference, query);
            }

            return new MaterializedViewExecution(queryExecution, reference);
        }
    }
}
