package org.rakam.postgresql.report;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.util.QueryFormatter;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class PostgresqlQueryExecutor implements QueryExecutor {
    private final static Logger LOGGER = Logger.get(PostgresqlQueryExecutor.class);
    public final static String MATERIALIZED_VIEW_PREFIX = "_materialized_";

    private final JDBCPoolDataSource connectionPool;
    protected static final ExecutorService QUERY_EXECUTOR = new ThreadPoolExecutor(0, 50, 120L, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new ThreadFactoryBuilder()
            .setNameFormat("postgresql-query-executor")
            .setUncaughtExceptionHandler((t, e) -> e.printStackTrace()).build());
    private final QueryMetadataStore queryMetadataStore;
    private final Metastore metastore;

    @Inject
    public PostgresqlQueryExecutor(@Named("store.adapter.postgresql") JDBCPoolDataSource connectionPool, Metastore metastore, QueryMetadataStore queryMetadataStore) {
        this.connectionPool = connectionPool;
        this.queryMetadataStore = queryMetadataStore;
        this.metastore = metastore;

        try (Connection connection = connectionPool.getConnection()) {
            connection.createStatement().execute("CREATE OR REPLACE FUNCTION to_unixtime(timestamp) RETURNS double precision" +
                    "    AS 'select extract(epoch from $1);'" +
                    "    LANGUAGE SQL" +
                    "    IMMUTABLE" +
                    "    RETURNS NULL ON NULL INPUT");
        } catch (SQLException e) {
            LOGGER.error(e, "Error while creating required Postgresql procedures.");
        }
    }

    @Override
    public QueryExecution executeRawQuery(String query) {
        return new PostgresqlQueryExecution(connectionPool, query, false);
    }

    @Override
    public QueryExecution executeRawStatement(String query) {
        return new PostgresqlQueryExecution(connectionPool, query, true);
    }

    @Override
    public String formatTableReference(String project, QualifiedName name) {
        if (name.getPrefix().isPresent()) {
            switch (name.getPrefix().get().toString()) {
                case "collection":
                    return project + "." + name.getSuffix();
                case "continuous":
                    final ContinuousQuery report = queryMetadataStore.getContinuousQuery(project, name.getSuffix());
                    StringBuilder builder = new StringBuilder();

                    new QueryFormatter(builder,
                            qualifiedName -> this.formatTableReference(project, qualifiedName))
                            .process(report.getQuery(), 1);

                    return "(" + builder.toString() + ") as " + name.getSuffix();
                case "materialized":
                    return project + "." + MATERIALIZED_VIEW_PREFIX + name.getSuffix();
                default:
                    throw new IllegalArgumentException("Schema does not exist: " + name.getPrefix().get().toString());
            }
        }

        if (name.getSuffix().equals("_all") && !name.getPrefix().isPresent()) {
            List<Map.Entry<String, List<SchemaField>>> collections = metastore.getCollections(project).entrySet().stream()
                    .filter(c -> !c.getKey().startsWith("_"))
                    .collect(Collectors.toList());
            if (!collections.isEmpty()) {
                String sharedColumns = collections.get(0).getValue().stream()
                        .filter(col -> collections.stream().allMatch(list -> list.getValue().contains(col)))
                        .map(f -> f.getName())
                        .collect(Collectors.joining(", "));

                return "(" + collections.stream().map(Map.Entry::getKey)
                        .map(collection -> format("select cast('%s' as text) as collection, %s from %s",
                                collection,
                                sharedColumns.isEmpty() ? "1" : sharedColumns,
                                project + "." + collection))
                        .collect(Collectors.joining(" union all ")) + ") _all";
            } else {
                return "(select null as collection, null as _user, null as _time limit 0) _all";
            }

        } else {
            return project + "." + name.getSuffix();
        }
    }

    public Connection getConnection() throws SQLException {
        return connectionPool.getConnection();
    }
}
