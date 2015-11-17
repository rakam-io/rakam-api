package org.rakam.report.postgresql;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.util.QueryFormatter;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PostgresqlQueryExecutor implements QueryExecutor {
    final static Logger LOGGER = Logger.get(PostgresqlQueryExecutor.class);
    public final static String MATERIALIZED_VIEW_PREFIX = "_materialized_";
    public final static String CONTINUOUS_QUERY_PREFIX = "_continuous_";

    private final JDBCPoolDataSource connectionPool;
    static final ExecutorService QUERY_EXECUTOR = new ThreadPoolExecutor(0, 50, 120L, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new ThreadFactoryBuilder()
            .setNameFormat("postgresql-query-executor")
            .setUncaughtExceptionHandler((t, e) -> e.printStackTrace()).build());
    private final Metastore metastore;
    private final QueryMetadataStore queryMetadataStore;

    @Inject
    public PostgresqlQueryExecutor(@Named("store.adapter.postgresql") JDBCPoolDataSource connectionPool, QueryMetadataStore queryMetadataStore, Metastore metastore) {
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

                    new QueryFormatter(builder, qualifiedName -> {
                        if(!qualifiedName.getPrefix().isPresent() && qualifiedName.getSuffix().equals("stream")) {
                            return replaceStream(report, metastore);
                        }
                        return project + "." + name.getSuffix();
                    }).process(report.query, 1);

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

    public static String replaceStream(ContinuousQuery report, Metastore metastore) {

        if (report.collections != null && report.collections.size() == 1) {
            return report.project + "." + report.collections.get(0);
        }

        final List<Map.Entry<String, List<SchemaField>>> collect = metastore.getCollections(report.project)
                .entrySet().stream()
                .filter(c -> report.collections == null || report.collections.size() == 0 || report.collections.contains(c.getKey()))
                .collect(Collectors.toList());

        Iterator<Map.Entry<String, List<SchemaField>>> entries = collect.iterator();

        List<SchemaField> base = null;
        while (entries.hasNext()) {
            Map.Entry<String, List<SchemaField>> next = entries.next();

            if (base == null) {
                base = new ArrayList(next.getValue());
                continue;
            }

            Iterator<SchemaField> iterator = base.iterator();
            while (iterator.hasNext()) {
                if (!next.getValue().contains(iterator.next())) {
                    iterator.remove();
                }
            }
        }

        String commonColumns = (base == null ? ImmutableList.<SchemaField>of() : base).stream().map(SchemaField::getName).collect(Collectors.joining(", "));

        return "(" + collect.stream().map(c -> String.format("select %s from %s.%s", commonColumns, report.project, c.getKey()))
                .collect(Collectors.joining(" union all ")) + ") as stream";
    }
}
