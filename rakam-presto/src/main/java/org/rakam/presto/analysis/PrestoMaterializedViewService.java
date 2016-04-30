package org.rakam.presto.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.util.QueryFormatter;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;

public class PrestoMaterializedViewService extends MaterializedViewService {
    public final static String MATERIALIZED_VIEW_PREFIX = "_materialized_";
    public final static SqlParser sqlParser = new SqlParser();
    private final static Logger LOGGER = Logger.get(PrestoMaterializedViewService.class);

    protected final QueryMetadataStore database;
    protected final QueryExecutor queryExecutor;
    private final DBI dbi;
    private static final double FILL_FACTOR = 0.75;
    private final PrestoMetastore metastore;
    private long maxShardRows = 1_000_000;
    private DataSize maxShardSize = new DataSize(256, MEGABYTE);

    @Inject
    public PrestoMaterializedViewService(@Named("presto.metastore.jdbc") JDBCPoolDataSource prestoMetastoreDataSource,
                                         QueryExecutor queryExecutor,
                                         PrestoMetastore metastore,
                                         QueryMetadataStore database) {
        super(database, queryExecutor);
        this.database = database;
        this.queryExecutor = queryExecutor;
        this.metastore = metastore;
        dbi = new DBI(prestoMetastoreDataSource);
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project, Optional<List<String>> names) {
        Stream<Map.Entry<String, List<SchemaField>>> views = metastore.getTables(project,
                tableColumn -> tableColumn.getTable().getTableName().startsWith(MATERIALIZED_VIEW_PREFIX)).entrySet().stream();
        if (names.isPresent()) {
            views = views.filter(e -> names.get().contains(e.getKey()));
        }

        return views.collect(Collectors.toMap(e -> e.getKey().substring(MATERIALIZED_VIEW_PREFIX.length()), e -> e.getValue()));
    }

    @Override
    public List<SchemaField> getSchema(String project, String tableName) {
        return metastore.getCollection(project, MATERIALIZED_VIEW_PREFIX + tableName);
    }

    @Override
    public CompletableFuture<Void> create(MaterializedView materializedView) {
        Statement statement = sqlParser.createStatement(materializedView.query);
        statement.accept(new DefaultTraversalVisitor<Void, Void>() {
            @Override
            protected Void visitSelect(Select node, Void context) {
                for (SelectItem selectItem : node.getSelectItems()) {
                    if (selectItem instanceof AllColumns) {
                        throw new RakamException("Wildcard in select items is not supported in materialized views.", BAD_REQUEST);
                    }
                    if (selectItem instanceof SingleColumn) {
                        SingleColumn selectColumn = (SingleColumn) selectItem;
                        if (!selectColumn.getAlias().isPresent() && !(selectColumn.getExpression() instanceof QualifiedNameReference)
                                && !(selectColumn.getExpression() instanceof DereferenceExpression)) {
                            throw new RakamException(format("Column '%s' must have alias", selectColumn.getExpression().toString()), BAD_REQUEST);
                        } else {
                            continue;
                        }
                    }

                    throw new IllegalStateException();
                }
                return null;
            }
        }, null);

        StringBuilder builder = new StringBuilder();
        new QueryFormatter(builder, qualifiedName -> queryExecutor.formatTableReference(materializedView.project, qualifiedName))
                .process(statement, 1);

        QueryExecution execution = queryExecutor
                .executeRawQuery(format("create table %s as %s limit 0",
                        queryExecutor.formatTableReference(materializedView.project,
                                QualifiedName.of("materialized", materializedView.tableName)), builder.toString()));

        return execution.getResult().thenAccept(result -> {
            if (result.isFailed()) {
                throw new RakamException(result.getError().message, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            } else {
                database.createMaterializedView(materializedView);
                return;
            }
        });
    }

    @Override
    public CompletableFuture<QueryResult> delete(String project, String name) {
        MaterializedView materializedView = database.getMaterializedView(project, name);
        database.deleteMaterializedView(project, name);
        String reference = queryExecutor.formatTableReference(materializedView.project, QualifiedName.of("materialized", materializedView.tableName));
        return queryExecutor.executeRawQuery(format("DROP TABLE %s",
                reference)).getResult().thenApply(result -> {
            if (result.isFailed()) {
                throw new RakamException("Error while deleting materialized table: " + result.getError().toString(), INTERNAL_SERVER_ERROR);
            }
            return result;
        });
    }

    @Override
    public MaterializedViewExecution lockAndUpdateView(MaterializedView materializedView) {
        CompletableFuture<Instant> f = new CompletableFuture<>();

        String tableName = queryExecutor.formatTableReference(materializedView.project,
                QualifiedName.of("materialized", materializedView.tableName));
        Query statement;
        synchronized (sqlParser) {
            statement = (Query) sqlParser.createStatement(materializedView.query);
        }

        if (!materializedView.incremental) {
            if (!materializedView.needsUpdate(Clock.systemUTC())) {
                return new MaterializedViewExecution(null, tableName);
            }
            QueryResult join = queryExecutor.executeRawQuery(format("DELETE FROM %s", tableName)).getResult().join();
            if (join.isFailed()) {
                throw new RakamException("Failed to delete table: " + join.getError().toString(), INTERNAL_SERVER_ERROR);
            }
            StringBuilder builder = new StringBuilder();
            new QueryFormatter(builder, name -> queryExecutor.formatTableReference(materializedView.project, name)).process(statement, 1);
            QueryExecution execution = queryExecutor.executeRawQuery(format("INSERT INTO %s %s", tableName, builder.toString()));
            return new MaterializedViewExecution(execution, tableName);
        } else {
            List<String> referencedCollections = new ArrayList<>();

            RakamSqlFormatter.formatSql(statement, name -> {
                if (name.getPrefix().map(prefix -> prefix.equals("collection")).orElse(true)) {
                    referencedCollections.add(name.getSuffix());
                }
                return null;
            });

            String materializedTableReference = tableName;

            Instant lastUpdated = materializedView.lastUpdate;
            Instant now = Instant.now();
            boolean needsUpdate = Duration.between(now, lastUpdated).compareTo(materializedView.updateInterval) > 0;

            QueryExecution queryExecution;
            if (needsUpdate && database.updateMaterializedView(materializedView, f)) {
                String query = RakamSqlFormatter.formatSql(statement,
                        name -> format("(SELECT * FROM %s WHERE \"$shard_time\" between timestamp '%s' and timestamp '%s')",
                                queryExecutor.formatTableReference(materializedView.project, name),
                                ISO_INSTANT.format(lastUpdated), ISO_INSTANT.format(now)));

                queryExecution = queryExecutor.executeRawStatement(format("INSERT INTO %s %s", materializedTableReference, query));
                final Instant finalLastUpdated = lastUpdated;
                queryExecution.getResult().thenAccept(result -> f.complete(!result.isFailed() ? finalLastUpdated : null));
            } else {
                queryExecution = QueryExecution.completedQueryExecution("", QueryResult.empty());
                f.complete(lastUpdated);
            }

            String reference;
            if (!needsUpdate) {
                reference = materializedTableReference;
            } else {
                String query = RakamSqlFormatter.formatSql(statement,
                        name -> format("(SELECT * FROM %s WHERE \"$shard_time\" > timestamp '%s'",
                                queryExecutor.formatTableReference(materializedView.project, name),
                                ISO_INSTANT.format(lastUpdated)));

                reference = format("(SELECT * from %s UNION ALL %s)", materializedTableReference, query);
            }

            return new MaterializedViewExecution(queryExecution, reference);
        }
    }

}
