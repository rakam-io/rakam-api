package org.rakam.postgresql.report;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QuerySampling;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.EXPECTATION_FAILED;
import static java.lang.String.format;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkLiteral;
import static org.rakam.util.ValidationUtil.checkTableColumn;

// forbid crosstab, dblink
public class PostgresqlQueryExecutor
        implements QueryExecutor
{
    private final static Logger LOGGER = Logger.get(PostgresqlQueryExecutor.class);
    public final static String MATERIALIZED_VIEW_PREFIX = "$materialized_";
    public final static String CONTINUOUS_QUERY_PREFIX = "$view_";

    private final JDBCPoolDataSource connectionPool;
    protected static final ExecutorService QUERY_EXECUTOR = Executors.newWorkStealingPool();
    private final Metastore metastore;
    private final boolean userServiceIsPostgresql;

    @Inject
    public PostgresqlQueryExecutor(@Named("store.adapter.postgresql") JDBCPoolDataSource connectionPool, Metastore metastore, @Named("user.storage.postgresql") boolean userServiceIsPostgresql)
    {
        this.connectionPool = connectionPool;
        this.metastore = metastore;
        this.userServiceIsPostgresql = userServiceIsPostgresql;

        try (Connection connection = connectionPool.getConnection()) {
            connection.createStatement().execute("CREATE OR REPLACE FUNCTION to_unixtime(timestamp) RETURNS double precision" +
                    "    AS 'select extract(epoch from $1);'" +
                    "    LANGUAGE SQL" +
                    "    IMMUTABLE" +
                    "    RETURNS NULL ON NULL INPUT");
        }
        catch (SQLException e) {
            LOGGER.error(e, "Error while creating required Postgresql procedures.");
        }
    }

    @Override
    public QueryExecution executeRawQuery(String query)
    {
        return new PostgresqlQueryExecution(connectionPool::getConnection, query, false);
    }

    @Override
    public QueryExecution executeRawStatement(String query)
    {
        return new PostgresqlQueryExecution(connectionPool::getConnection, query, true);
    }

    @Override
    public String formatTableReference(String project, QualifiedName name, Optional<QuerySampling> sample, Map<String, String> sessionParameters, String defaultSchema)
    {
        if (name.getPrefix().isPresent()) {
            switch (name.getPrefix().get().toString()) {
                case "collection":
                    return project + "." + checkCollection(name.getSuffix()) +
                            sample.map(e -> " TABLESAMPLE " + e.method.name() + "(" + e.percentage + ")").orElse("");
                case "continuous":
                    return project + "." + checkCollection(CONTINUOUS_QUERY_PREFIX + name.getSuffix());
                case "materialized":
                    return project + "." + checkCollection(MATERIALIZED_VIEW_PREFIX + name.getSuffix());
                default:
                    throw new RakamException("Schema does not exist: " + name.getPrefix().get().toString(), BAD_REQUEST);
            }
        }
        else if (name.getSuffix().equals("users") || name.getSuffix().equals("_users")) {
            if (userServiceIsPostgresql) {
                return project + ".users";
            }
            throw new RakamException("User implementation is not supported", EXPECTATION_FAILED);
        }

        if (name.getSuffix().equals("_all") && !name.getPrefix().isPresent()) {
            List<Map.Entry<String, List<SchemaField>>> collections = metastore.getCollections(project).entrySet().stream()
                    .collect(Collectors.toList());
            if (!collections.isEmpty()) {
                String sharedColumns = collections.get(0).getValue().stream()
                        .filter(col -> collections.stream().allMatch(list -> list.getValue().contains(col)))
                        .map(f -> checkTableColumn(f.getName()))
                        .collect(Collectors.joining(", "));

                return "(" + collections.stream().map(Map.Entry::getKey)
                        .map(collection -> format("select cast('%s' as text) as \"$collection\" %s, row_to_json(t) properties from %s t",
                                checkLiteral(collection),
                                sharedColumns.isEmpty() ? "" : (", " + sharedColumns),
                                project + "." + checkCollection(collection)))
                        .collect(Collectors.joining(" union all \n")) + ") _all";
            }
            else {
                return "(select cast(null as text) as \"$collection\", cast(null as text) as _user, cast(null as timestamp) as _time limit 0) _all";
            }
        }
        else {
            return project + "." + checkCollection(name.getSuffix());
        }
    }

    public Connection getConnection()
            throws SQLException
    {
        return connectionPool.getConnection();
    }
}
