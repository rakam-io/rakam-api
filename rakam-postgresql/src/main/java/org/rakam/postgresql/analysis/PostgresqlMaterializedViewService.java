package org.rakam.postgresql.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.RequestContext;
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
import java.util.function.Supplier;

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
    public CompletableFuture<Void> create(RequestContext context, MaterializedView materializedView) {
        String format;
        try {
            materializedView.validateQuery();

            StringBuilder builder = new StringBuilder();
            Query statement = (Query) SqlUtil.parseSql(materializedView.query);

            new RakamSqlFormatter.Formatter(builder, name -> queryExecutor
                    .formatTableReference(context.project, name, Optional.empty(), new HashMap<String, String>() {
                        @Override
                        public String put(String key, String value) {
                            CustomDataSource read = JsonHelper.read(value, CustomDataSource.class);
                            if (!ImmutableList.of("postgresql", "mysql").contains(read.type)) {
                                throw new RakamException("Cross database materialized views are not supported in Postgresql deployment type.", BAD_REQUEST);
                            }

                            throw new RakamException("Cross database materialized views are not supported in Postgresql deployment type.", BAD_REQUEST);
                        }
                    }), '"').process(statement, 1);

            if (!materializedView.incremental) {
                format = format("CREATE MATERIALIZED VIEW %s.%s AS %s WITH NO DATA",
                        checkProject(context.project, '"'), checkCollection(MATERIALIZED_VIEW_PREFIX + materializedView.tableName), builder.toString());
            } else {
                format = format("CREATE TABLE %s.%s AS %s WITH NO DATA",
                        checkProject(context.project, '"'), checkCollection(MATERIALIZED_VIEW_PREFIX + materializedView.tableName), builder.toString());
            }
        } catch (Exception e) {
            CompletableFuture<Void> f = new CompletableFuture<>();
            f.completeExceptionally(e);
            return f;
        }

        return queryExecutor.executeRawStatement(context, format).getResult().thenAccept(result -> {
            if (result.isFailed()) {
                try {
                    get(context.project, materializedView.tableName);
                    throw new AlreadyExistsException("Materialized view", BAD_REQUEST);
                } catch (NotExistsException e) {
                }

                if (!"42P07".equals(result.getError().sqlState)) {
                    throw new RakamException("Couldn't created table: " +
                            result.getError().toString(), BAD_REQUEST);
                }
            }

            database.createMaterializedView(context.project, materializedView);
        });
    }

    @Override
    public CompletableFuture<QueryResult> delete(RequestContext context, String name) {
        MaterializedView materializedView = database.getMaterializedView(context.project, name);

        String type = materializedView.incremental ? "TABLE" : "MATERIALIZED VIEW";
        QueryExecution queryExecution = queryExecutor.executeRawStatement(context, format("DROP %s %s.%s",
                type,
                checkProject(context.project, '"'), checkCollection(MATERIALIZED_VIEW_PREFIX + materializedView.tableName)));
        return queryExecution.getResult().thenApply(result -> {
            if (!result.isFailed())
                database.deleteMaterializedView(context.project, name);
            else
                return result;

            return QueryResult.empty();
        });
    }

    @Override
    public MaterializedViewExecution lockAndUpdateView(RequestContext context, MaterializedView materializedView) {
        CompletableFuture<Instant> f = new CompletableFuture<>();

        String tableName = queryExecutor.formatTableReference(context.project,
                QualifiedName.of("materialized", materializedView.tableName), Optional.empty(), ImmutableMap.of());
        Query statement = (Query) SqlUtil.parseSql(materializedView.query);

        Map<String, String> sessionProperties = new HashMap<>();
        if (!materializedView.incremental) {
            if (!materializedView.needsUpdate(clock) || !database.updateMaterializedView(context.project, materializedView, f)) {
                f.complete(null);
                return new MaterializedViewExecution(null, tableName, false);
            }

            String collection = checkCollection(MATERIALIZED_VIEW_PREFIX + materializedView.tableName);
            QueryExecution execution = queryExecutor.executeRawStatement(context, format("REFRESH MATERIALIZED VIEW %s.%s ",
                    context.project, collection));
            return new MaterializedViewExecution(() -> new DelegateQueryExecution(execution, result -> {
                f.complete(!result.isFailed() ? Instant.now() : null);
                return result;
            }), tableName, materializedView.lastUpdate == null);
        } else {
            String materializedTableReference = tableName;

            boolean willBeUpdated = materializedView.needsUpdate(clock) && database.updateMaterializedView(context.project, materializedView, f);

            Supplier<QueryExecution> queryExecution;
            Instant now = queryExecutor.runRawQuery("select now()", resultSet -> {
                resultSet.next();
                return resultSet.getTimestamp(1).toInstant();
            });

            if (willBeUpdated) {
                String query = formatSql(statement,
                        name -> {
                            String predicate = materializedView.lastUpdate != null ? format("between timestamp with time zone '%s' and timestamp with time zone '%s'",
                                    materializedView.lastUpdate.toString(), now.toString()) : format(" < timestamp with time zone '%s'", now.toString());

                            String collection = queryExecutor.formatTableReference(context.project, name, Optional.empty(),
                                    ImmutableMap.of());
                            return format("(SELECT * FROM %s WHERE \"$server_time\" %s) data", collection, predicate);
                        }, '"');

                queryExecution = () -> {
                    QueryExecution execution = queryExecutor.executeRawStatement(
                            context, format("INSERT INTO %s %s", materializedTableReference, query), sessionProperties);
                    execution.getResult().thenAccept(result -> f.complete(!result.isFailed() ? now : null));
                    return execution;
                };
            } else {
                f.complete(materializedView.lastUpdate);
                queryExecution = null;
            }

            String reference;
            if (!materializedView.realTime || materializedView.lastUpdate == null) {
                reference = materializedTableReference;
            } else {
                String query = formatSql(statement,
                        name -> {
                            String collection = format("(SELECT * FROM %s %s) data",
                                    queryExecutor.formatTableReference(context.project, name, Optional.empty(), ImmutableMap.of()),
                                    format("WHERE \"$server_time\" > timestamp with time zone '%s'", now.toString()));
                            return collection;
                        }, '"');

                reference = format("(SELECT * from %s UNION ALL %s) data", materializedTableReference, query);
            }

            // PG doesn't support UNCOMMITTED transactions so if the INSERT queries happens to finish before the SELECT,
            // the user will see duplicate data
            return new MaterializedViewExecution(queryExecution, reference, true);
        }
    }
}
