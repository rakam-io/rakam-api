package org.rakam.presto.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import javax.inject.Inject;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.sql.RakamSqlFormatter.formatSql;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.String.format;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class PrestoMaterializedViewService
        extends MaterializedViewService
{
    public final static String MATERIALIZED_VIEW_PREFIX = "$materialized_";
    public final static SqlParser sqlParser = new SqlParser();
    protected final QueryMetadataStore database;
    protected final QueryExecutor queryExecutor;
    private final Metastore metastore;
    private final PrestoConfig prestoConfig;

    @Inject
    public PrestoMaterializedViewService(
            PrestoConfig prestoConfig,
            QueryExecutor queryExecutor,
            Metastore metastore,
            QueryMetadataStore database)
    {
        super(database, queryExecutor, '"');
        this.database = database;
        this.prestoConfig = prestoConfig;
        this.queryExecutor = queryExecutor;
        this.metastore = metastore;
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project, Optional<List<String>> names)
    {
        Stream<Map.Entry<String, List<SchemaField>>> views = metastore.getCollections(project).entrySet()
                .stream().filter(e -> e.getKey().startsWith(MATERIALIZED_VIEW_PREFIX));
        if (names.isPresent()) {
            views = views.filter(e -> names.get().contains(e.getKey()));
        }

        return views.collect(Collectors.toMap(e -> e.getKey().substring(MATERIALIZED_VIEW_PREFIX.length()), e -> e.getValue()));
    }

    @Override
    public List<SchemaField> getSchema(String project, String tableName)
    {
        return metastore.getCollection(project, MATERIALIZED_VIEW_PREFIX + tableName);
    }

    @Override
    public CompletableFuture<Void> create(String project, MaterializedView materializedView)
    {
        Query statement = (Query) sqlParser.createStatement(materializedView.query);
        QuerySpecification queryBody = (QuerySpecification) statement.getQueryBody();
        List<SelectItem> selectItems = queryBody.getSelect().getSelectItems();
        if (selectItems.stream().anyMatch(e -> e instanceof AllColumns)) {
            throw new RakamException("Wildcard in select items is not supported in materialized views.", BAD_REQUEST);
        }

        for (SelectItem selectItem : selectItems) {
            SingleColumn selectColumn = (SingleColumn) selectItem;
            if (!selectColumn.getAlias().isPresent() && !(selectColumn.getExpression() instanceof QualifiedNameReference)
                    && !(selectColumn.getExpression() instanceof DereferenceExpression)) {
                throw new RakamException(format("Column '%s' must have alias", selectColumn.getExpression().toString()), BAD_REQUEST);
            }
        }

        StringBuilder builder = new StringBuilder();
        HashMap<String, String> map = new HashMap<>();
        new RakamSqlFormatter.Formatter(builder, qualifiedName -> queryExecutor.formatTableReference(project, qualifiedName, Optional.empty(), map, "collection"), '"')
                .process(statement, 1);

        QueryExecution execution = queryExecutor
                .executeRawStatement(format("create table %s as %s limit 0",
                        queryExecutor.formatTableReference(project,
                                QualifiedName.of("materialized", materializedView.tableName), Optional.empty(), ImmutableMap.of(), "collection"), builder.toString(), Optional.empty()), map);

        return execution.getResult().thenAccept(result -> {
            if (result.isFailed()) {
                throw new RakamException(result.getError().message, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            }
            else {
                database.createMaterializedView(project, materializedView);
            }
        });
    }

    @Override
    public CompletableFuture<QueryResult> delete(String project, String name)
    {
        MaterializedView materializedView = database.getMaterializedView(project, name);
        database.deleteMaterializedView(project, name);
        String reference = queryExecutor.formatTableReference(project, QualifiedName.of("materialized", materializedView.tableName), Optional.empty(), ImmutableMap.of(), "collection");
        return queryExecutor.executeRawQuery(format("DROP TABLE %s", reference)).getResult().thenApply(result -> {
            if (result.isFailed()) {
                throw new RakamException("Error while deleting materialized table: " + result.getError().toString(), INTERNAL_SERVER_ERROR);
            }
            return result;
        });
    }

    @Override
    public MaterializedViewExecution lockAndUpdateView(String project, MaterializedView materializedView)
    {
        CompletableFuture<Instant> f = new CompletableFuture<>();

        String tableName = queryExecutor.formatTableReference(project,
                QualifiedName.of("materialized", materializedView.tableName), Optional.empty(), ImmutableMap.of(), "collection");
        Query statement = (Query) sqlParser.createStatement(materializedView.query);

        Map<String, String> sessionProperties = new HashMap<>();
        if (!materializedView.incremental) {
            if (!database.updateMaterializedView(project, materializedView, f)) {
                return new MaterializedViewExecution(null, tableName);
            }

            QueryResult join = queryExecutor.executeRawQuery(format("DELETE FROM %s", tableName)).getResult().join();
            if (join.isFailed()) {
                throw new RakamException("Failed to delete table: " + join.getError().toString(), INTERNAL_SERVER_ERROR);
            }
            StringBuilder builder = new StringBuilder();

            new RakamSqlFormatter.Formatter(builder, name -> queryExecutor.formatTableReference(project, name, Optional.empty(), sessionProperties, "collection"), '"').process(statement, 1);
            QueryExecution execution = queryExecutor.executeRawStatement(format("INSERT INTO %s %s", tableName, builder.toString()), sessionProperties);
            execution.getResult().thenAccept(result -> f.complete(!result.isFailed() ? Instant.now() : null));
            return new MaterializedViewExecution(execution, tableName);
        }
        else {
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

            QueryExecution queryExecution;
            if (database.updateMaterializedView(project, materializedView, f)) {
                String query = formatSql(statement,
                        name -> {
                            String predicate = lastUpdated != null ? String.format("between from_unixtime(%d) and from_unixtime(%d)",
                                    lastUpdated.getEpochSecond(), now.getEpochSecond()) :
                                    String.format(" < from_unixtime(%d)", now.getEpochSecond());

                            return format("(SELECT * FROM %s WHERE %s %s)",
                                    queryExecutor.formatTableReference(project, name, Optional.empty(), sessionProperties, "collection"),
                                    checkTableColumn(prestoConfig.getCheckpointColumn()),
                                    predicate);
                        }, '"');

                queryExecution = queryExecutor.executeRawStatement(format("INSERT INTO %s %s", materializedTableReference, query), sessionProperties);
                queryExecution.getResult().thenAccept(result -> f.complete(!result.isFailed() ? now : null));
            }
            else {
                queryExecution = QueryExecution.completedQueryExecution("", QueryResult.empty());
                f.complete(lastUpdated);
            }

            String reference;
            if (!materializedView.realTime) {
                reference = materializedTableReference;
            }
            else {
                String query = formatSql(statement,
                        name -> {
                            String collection = format("(SELECT * FROM %s %s) data",
                                    queryExecutor.formatTableReference(project, name, Optional.empty(), ImmutableMap.of(), "collection"),
                                    format("WHERE %s > from_unixtime(%d)",
                                            checkTableColumn(prestoConfig.getCheckpointColumn()),
                                            (lastUpdated != null ? lastUpdated : now).getEpochSecond()));
                            return collection;
                        }, '"');

                reference = format("(SELECT * from %s UNION ALL %s)", materializedTableReference, query);
            }

            return new MaterializedViewExecution(queryExecution, reference);
        }
    }
}
