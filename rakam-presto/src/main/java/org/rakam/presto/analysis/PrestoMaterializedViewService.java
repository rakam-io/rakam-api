package org.rakam.presto.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.RequestContext;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.ChainQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.NotExistsException;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.sql.RakamSqlFormatter.formatSql;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.String.format;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class PrestoMaterializedViewService
        extends MaterializedViewService {
    public final static String MATERIALIZED_VIEW_PREFIX = "$materialized_";
    public final static SqlParser sqlParser = new SqlParser();
    protected final QueryMetadataStore database;
    protected final QueryExecutor queryExecutor;
    private final PrestoAbstractMetastore metastore;
    private final PrestoConfig prestoConfig;
    private final Clock clock;

    @Inject
    public PrestoMaterializedViewService(
            PrestoConfig prestoConfig,
            QueryExecutor queryExecutor,
            PrestoAbstractMetastore metastore,
            QueryMetadataStore database,
            Clock clock) {
        super(database, queryExecutor, '"');
        this.database = database;
        this.prestoConfig = prestoConfig;
        this.queryExecutor = queryExecutor;
        this.metastore = metastore;
        this.clock = clock;
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(RequestContext context, Optional<List<String>> names) {
        Stream<Map.Entry<String, List<SchemaField>>> views = metastore.getSchemas(context.project, e -> e.startsWith(MATERIALIZED_VIEW_PREFIX))
                .entrySet()
                .stream().filter(e -> e.getKey().startsWith(MATERIALIZED_VIEW_PREFIX));
        if (names.isPresent()) {
            views = views.filter(e -> names.get().contains(e.getKey()));
        }

        return views.collect(Collectors.toMap(e -> e.getKey().substring(MATERIALIZED_VIEW_PREFIX.length()), e -> e.getValue()));
    }

    @Override
    public List<SchemaField> getSchema(RequestContext context, String tableName) {
        return metastore.getCollection(context.project, MATERIALIZED_VIEW_PREFIX + tableName);
    }

    @Override
    public CompletableFuture<Void> create(RequestContext context, MaterializedView materializedView) {
        Query statement = (Query) sqlParser.createStatement(materializedView.query, new ParsingOptions());

        StringBuilder builder = new StringBuilder();
        HashMap<String, String> map = new HashMap<>();
        new RakamSqlFormatter.Formatter(builder, qualifiedName -> queryExecutor.formatTableReference(context.project, qualifiedName, Optional.empty(), map), '"')
                .process(statement, 1);

        QueryExecution execution = queryExecutor
                .executeRawStatement(context, format("create table %s as %s limit 0",
                        queryExecutor.formatTableReference(context.project,
                                QualifiedName.of("materialized", materializedView.tableName), Optional.empty(), ImmutableMap.of()), builder.toString(), Optional.empty()), map);

        return execution.getResult().thenAccept(result -> {
            try {
                get(context.project, materializedView.tableName);
                throw new AlreadyExistsException("Materialized view", BAD_REQUEST);
            } catch (NotExistsException e) {
            }

            if (result.isFailed()) {
                throw new RakamException(result.getError().message, INTERNAL_SERVER_ERROR);
            } else {
                database.createMaterializedView(context.project, materializedView);
            }
        });
    }

    @Override
    public CompletableFuture<QueryResult> delete(RequestContext context, String name) {
        MaterializedView materializedView = database.getMaterializedView(context.project, name);
        database.deleteMaterializedView(context.project, name);
        String reference = queryExecutor.formatTableReference(context.project, QualifiedName.of("materialized", materializedView.tableName), Optional.empty(), ImmutableMap.of());
        return queryExecutor.executeRawQuery(context, format("DROP TABLE %s", reference)).getResult().thenApply(result -> {
            if (result.isFailed()) {
                throw new RakamException("Error while deleting materialized table: " + result.getError().toString(), INTERNAL_SERVER_ERROR);
            }
            return result;
        });
    }

    @Override
    public MaterializedViewExecution lockAndUpdateView(RequestContext context, MaterializedView materializedView) {
        String tableName = queryExecutor.formatTableReference(context.project,
                QualifiedName.of("materialized", materializedView.tableName), Optional.empty(), ImmutableMap.of());
        Query statement = (Query) sqlParser.createStatement(materializedView.query);

        Map<String, String> sessionProperties = new HashMap<>();
        if (!materializedView.incremental) {
            CompletableFuture<Instant> f = new CompletableFuture<>();

            if (!materializedView.needsUpdate(clock) || !database.updateMaterializedView(context.project, materializedView, f)) {
                f.complete(null);
                return new MaterializedViewExecution(null, tableName, false);
            }

            StringBuilder builder = new StringBuilder();
            new RakamSqlFormatter.Formatter(builder, name ->
                    queryExecutor.formatTableReference(context.project, name, Optional.empty(),
                            sessionProperties), '"').process(statement, 1);

            return new MaterializedViewExecution(() -> {
                // TODO: make this atomic
                QueryExecution deleteData = queryExecutor.executeRawStatement(context, format("DELETE FROM %s", tableName));
                return new ChainQueryExecution(ImmutableList.of(deleteData), queryResults -> {
                    QueryExecution execution = queryExecutor.executeRawStatement(context, format("INSERT INTO %s %s", tableName, builder.toString()), sessionProperties);
                    execution.getResult().whenComplete((result, ex) -> f.complete(ex == null && !result.isFailed() ? Instant.now() : null));
                    return execution;
                });
            }, tableName, true);
        } else {
            CompletableFuture<Instant> f = new CompletableFuture<>();

            List<String> referencedCollections = new ArrayList<>();

            formatSql(statement, name -> {
                if (name.getPrefix().map(prefix -> prefix.equals("collection")).orElse(true)) {
                    referencedCollections.add(name.getSuffix());
                }
                return null;
            }, '"');

            String materializedTableReference = tableName;

            Instant lastUpdated = materializedView.lastUpdate;
            Instant now = Instant.now();

            Supplier<QueryExecution> runnable;
            if (materializedView.needsUpdate(clock) && database.updateMaterializedView(context.project, materializedView, f)) {
                String query = formatSql(statement,
                        name -> {
                            String predicate = lastUpdated != null ? String.format("between from_unixtime(%d) and from_unixtime(%d)",
                                    lastUpdated.getEpochSecond(), now.getEpochSecond()) :
                                    String.format(" < from_unixtime(%d)", now.getEpochSecond());

                            return format("(SELECT * FROM %s WHERE %s %s)",
                                    queryExecutor.formatTableReference(context.project, name, Optional.empty(), sessionProperties),
                                    checkTableColumn(prestoConfig.getCheckpointColumn()),
                                    predicate);
                        }, '"');

                runnable = () -> {
                    QueryExecution execution = queryExecutor.executeRawStatement(context, format("INSERT INTO %s %s", materializedTableReference, query), sessionProperties);
                    execution.getResult().whenComplete((result, ex) -> f.complete(ex == null && !result.isFailed() ? Instant.now() : null));
                    return execution;
                };
            } else {
                runnable = null;
            }

            String reference;
            if (!materializedView.realTime || lastUpdated == null) {
                reference = materializedTableReference;
            } else {
                String query = formatSql(statement,
                        name -> {
                            String collection = format("(SELECT * FROM %s %s) data",
                                    queryExecutor.formatTableReference(context.project, name, Optional.empty(), ImmutableMap.of()),
                                    format("WHERE %s > from_unixtime(%d)",
                                            checkTableColumn(prestoConfig.getCheckpointColumn()), lastUpdated.getEpochSecond()));
                            return collection;
                        }, '"');

                reference = format("(SELECT * from %s UNION ALL %s)", materializedTableReference, query);
            }

            return new MaterializedViewExecution(runnable, reference, materializedView.lastUpdate == null);
        }
    }
}
