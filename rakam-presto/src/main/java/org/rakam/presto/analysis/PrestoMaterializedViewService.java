package org.rakam.presto.analysis;

import com.facebook.presto.raptor.util.UuidUtil;
import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.inject.name.Named;
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.String.format;
import static java.util.Collections.nCopies;

public class PrestoMaterializedViewService extends MaterializedViewService {
    public final static String MATERIALIZED_VIEW_PREFIX = "_materialized_";
    public final static SqlParser sqlParser = new SqlParser();

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
        if(names.isPresent()) {
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
        final boolean[] incrementalFieldExists = {false};

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
                        if (!selectColumn.getAlias().isPresent() && !(selectColumn.getExpression() instanceof QualifiedNameReference)) {
                            throw new RakamException(format("Column '%s' must have alias", selectColumn.getExpression().toString()), BAD_REQUEST);
                        } else {
                            if (materializedView.incrementalField != null) {
                                if (incrementalFieldExists[0]) {
                                    throw new RakamException("Incremental field must be referenced only once", BAD_REQUEST);
                                }
                                incrementalFieldExists[0] = true;
                            }
                            continue;
                        }
                    }

                    throw new IllegalStateException();
                }
                return null;
            }
        }, null);

        if (materializedView.incrementalField != null && !incrementalFieldExists[0]) {
            throw new RakamException("Incremental field must be referenced in query.", BAD_REQUEST);
        }

        StringBuilder builder = new StringBuilder();
        new QueryFormatter(builder, qualifiedName -> queryExecutor.formatTableReference(materializedView.project, qualifiedName))
                .process(statement, 1);

        QueryExecution execution = queryExecutor
                .executeRawQuery(String.format("create table %s as %s limit 0",
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

        Statement queryStatement = sqlParser.createStatement(materializedView.query);

        List<String> referencedCollections = new ArrayList<>();

        RakamSqlFormatter.formatSql(queryStatement, name -> {
            if (name.getPrefix().map(prefix -> prefix.equals("collection")).orElse(true)) {
                referencedCollections.add(name.getSuffix());
            }
            return null;
        });

        String materializedTableReference = queryExecutor.formatTableReference(materializedView.project,
                QualifiedName.of("materialized", materializedView.tableName));

        Map<String, List<UUID>> shardsNeedsToBeProcessed = new HashMap<>();
        Map<String, List<UUID>> shardsWillBeProcessed = new HashMap<>();
        Instant lastUpdated = null;
        Instant now = Instant.now();

        try (Connection connection = dbi.open().getConnection()) {
            String sql = String.format(
                    "select tables.table_name, shard_uuid, create_time, (compressed_size >= %f or row_count >= %f or create_time <= now() - interval 1 day) as status \n" +
                            "from shards join tables on (tables.table_id = shards.table_id) \n" +
                            "where tables.schema_name = ? and tables.table_name in (%s) \n" +
                            (materializedView.lastUpdate != null ? " and create_time > ?" : "") +
                            " order by create_time",
                    FILL_FACTOR * maxShardSize.toBytes(),
                    FILL_FACTOR * maxShardRows,
                    Joiner.on(",").join(nCopies(referencedCollections.size(), "?")));

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, materializedView.project);

                for (int i = 0; i < referencedCollections.size(); i++) {
                    statement.setString(i + 2, referencedCollections.get(i));
                }

                if (materializedView.lastUpdate != null) {
                    statement.setTimestamp(referencedCollections.size() + 2, Timestamp.from(materializedView.lastUpdate));
                }

                try (ResultSet resultSet = statement.executeQuery()) {
                    boolean switched = false;
                    while (resultSet.next()) {
                        String tableName = resultSet.getString(1);
                        UUID shardUuid = UuidUtil.uuidFromBytes(resultSet.getBytes(2));

                        if (!resultSet.getBoolean(4)) {
                            if (!switched) {
                                lastUpdated = resultSet.getTimestamp(3).toInstant();
                            }
                            switched = true;

                        }
                        (switched ? shardsWillBeProcessed : shardsNeedsToBeProcessed)
                                .computeIfAbsent(tableName, (key) -> new ArrayList<>()).add(shardUuid);
                    }
                }
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }

        if (lastUpdated == null && shardsWillBeProcessed.isEmpty()) {
            lastUpdated = now;
        }

        QueryExecution queryExecution;
        if (!shardsNeedsToBeProcessed.isEmpty() && database.updateMaterializedView(materializedView, f)) {
            String query = RakamSqlFormatter.formatSql(queryStatement,
                    new ShardPredicateTableNameMapper(shardsNeedsToBeProcessed, materializedView));

            queryExecution = queryExecutor.executeRawStatement(String.format("INSERT INTO %s %s", materializedTableReference, query));
            final Instant finalLastUpdated = lastUpdated;
            queryExecution.getResult().thenAccept(result -> f.complete(!result.isFailed() ? finalLastUpdated : null));
        } else {
            queryExecution = QueryExecution.completedQueryExecution("", QueryResult.empty());
            f.complete(lastUpdated);
        }

        String reference;
        if (shardsWillBeProcessed.isEmpty()) {
            reference = materializedTableReference;
        } else {
            String query = RakamSqlFormatter.formatSql(queryStatement,
                    new ShardPredicateTableNameMapper(shardsWillBeProcessed, materializedView));

            reference = String.format("(SELECT * from %s UNION ALL %s)", materializedTableReference, query);
        }

        return new MaterializedViewExecution(queryExecution, reference);
    }

    private class ShardPredicateTableNameMapper implements Function<QualifiedName, String> {
        private final Map<String, List<UUID>> shards;
        private final MaterializedView materializedView;

        public ShardPredicateTableNameMapper(Map<String, List<UUID>> shards, MaterializedView materializedView) {
            this.shards = shards;
            this.materializedView = materializedView;
        }

        @Override
        public String apply(QualifiedName name) {
            List<UUID> uuids = shards.get(name.getSuffix());
            String predicate;
            if (uuids == null || uuids.isEmpty()) {
                predicate = "is null";
            } else {
                predicate = String.format("in (%s)",
                        uuids.stream().map(uuid -> "'" + uuid.toString() + "'").collect(Collectors.joining(", ")));
            }
            return String.format("(SELECT * FROM %s WHERE \"$shard_uuid\" %s)",
                    queryExecutor.formatTableReference(materializedView.project, name),
                    predicate);
        }
    }
}
