package org.rakam.postgresql.report;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.util.QueryFormatter;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PostgresqlQueryExecutor implements QueryExecutor {
    final static Logger LOGGER = Logger.get(PostgresqlQueryExecutor.class);
    public final static String MATERIALIZED_VIEW_PREFIX = "_materialized_";

    private final JDBCPoolDataSource connectionPool;
    static final ExecutorService QUERY_EXECUTOR = new ThreadPoolExecutor(0, 50, 120L, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new ThreadFactoryBuilder()
            .setNameFormat("postgresql-query-executor")
            .setUncaughtExceptionHandler((t, e) -> e.printStackTrace()).build());
    private final QueryMetadataStore queryMetadataStore;

    @Inject
    public PostgresqlQueryExecutor(@Named("store.adapter.postgresql") JDBCPoolDataSource connectionPool, QueryMetadataStore queryMetadataStore) {
        this.connectionPool = connectionPool;
        this.queryMetadataStore = queryMetadataStore;

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
                    if(report == null) {
                        throw new RakamException(String.format("Continuous query table %s is not found", name.getSuffix()), HttpResponseStatus.BAD_REQUEST);
                    }
                    StringBuilder builder = new StringBuilder();

                    new QueryFormatter(builder,
                            qualifiedName -> this.formatTableReference(project, qualifiedName))
                            .process(report.getQuery(), 1);

                    return "("+builder.toString()+") as "+name.getSuffix();
                case "materialized":
                    return project + "." + MATERIALIZED_VIEW_PREFIX + name.getSuffix();
                default:
                    throw new IllegalArgumentException("Schema does not exist: " + name.getPrefix().get().toString());
            }
        }
        return project + "." + name.getSuffix();
    }

    public Connection getConnection() throws SQLException {
        return connectionPool.getConnection();
    }
}
