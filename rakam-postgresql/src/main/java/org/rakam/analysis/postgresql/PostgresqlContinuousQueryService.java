package org.rakam.analysis.postgresql;

import com.facebook.presto.sql.SQLFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.report.QueryResult;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;
import org.rakam.util.JsonHelper;
import org.rakam.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/04/15 02:34.
 */
public class PostgresqlContinuousQueryService extends ContinuousQueryService {
    final static Logger LOGGER = LoggerFactory.getLogger(PostgresqlContinuousQueryService.class);

    private final QueryMetadataStore reportDatabase;
    private final PostgresqlQueryExecutor executor;
    private final Metastore metastore;

    @Inject
    public PostgresqlContinuousQueryService(Metastore metastore, QueryMetadataStore reportDatabase, PostgresqlQueryExecutor executor) {
        super(reportDatabase);
        this.reportDatabase = reportDatabase;
        this.metastore = metastore;
        this.executor = executor;

        ScheduledExecutorService updater = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setUncaughtExceptionHandler((t, e) ->
                        LOGGER.error("Error while updating continuous query table.", e))
                .build());
        updater.execute(() -> executor.executeRawQuery("select pg_advisory_lock(8888)").getResult().thenAccept(result -> {
            if(!result.isFailed()) {
                // we obtained the lock so we're the master node.
                LOGGER.info("Became the master node. Scheduling periodic table updates for materialized and continuous queries.");
                updater.scheduleAtFixedRate(this::updateTable, 10, 10, TimeUnit.SECONDS);
            }else {
                LOGGER.error("Error while obtaining lock from Postgresql: {}", result.getError());
            }
        }));
    }

    private String replaceSourceTable(String query, String sampleCollection) {
        Statement statement = new SqlParser().createStatement(query);
        StringBuilder builder = new StringBuilder();
        statement.accept(new SQLFormatter.Formatter(builder) {
            @Override
            protected Void visitTable(Table node, Integer indent) {
                if(node.getName().getSuffix().equals("stream")) {
                    builder.append(sampleCollection);
                    return null;
                }else {
                    return super.visitTable(node, indent);
                }
            }
        }, 0);
        return builder.toString();
    }

    @Override
    public CompletableFuture<QueryResult> create(ContinuousQuery report) {
        Map<String, PostgresqlFunction> continuousQueryMetadata = processQuery(report);
        if(report.collections.isEmpty()) {
            CompletableFuture<QueryResult> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalArgumentException("Continuous query must have at least one collection"));
            return f;
        }

        // just to create the table with the columns.
        String query = format("create table %s.%s as (%s limit 0)", report.project,
                report.getTableName(),
                replaceSourceTable(report.query, report.project+"."+report.collections.get(0)));

        if(report.options == null) {
            report = new ContinuousQuery(report.project,
                    report.name, report.tableName,
                    report.query, report.collections, ImmutableMap.of("_metadata", continuousQueryMetadata));
        }else {
            if(report.options.containsKey("_metadata")) {
                throw new IllegalArgumentException("_metadata option is reserved");
            }

            try {
                report.options.put("_metadata", continuousQueryMetadata);
            } catch (UnsupportedOperationException e) {
                // the map seems to be immutable.
                HashMap<String, Object> map = new HashMap<>(report.options);
                map.put("_metadata", continuousQueryMetadata);

                report = new ContinuousQuery(report.project,
                        report.name, report.tableName,
                        report.query, report.collections, map);
            }
        }
        final ContinuousQuery finalReport = report;
        return executor.executeRawStatement(query).getResult().thenApply(result -> {
            if(!result.isFailed()) {
                reportDatabase.createContinuousQuery(finalReport);

                String groupings = continuousQueryMetadata.entrySet().stream()
                        .filter(e -> e.getValue() == null)
                        .map(e -> e.getKey())
                        .collect(Collectors.joining(", "));
                executor.executeRawStatement(format("ALTER TABLE %s.%s ADD PRIMARY KEY (%s)",
                        finalReport.project, finalReport.getTableName(), groupings)).getResult().thenAccept(indexResult -> {
                    if (indexResult.isFailed()) {
                        LOGGER.error("Failed to create unique index on continuous column: {0}", result.getError());
                    }
                });

            }
            return result;
        });
    }

    @Override
    public CompletableFuture<QueryResult> delete(String project, String name) {
        ContinuousQuery continuousQuery = reportDatabase.getContinuousQuery(project, name);

        String prestoQuery = format("drop table continuous.%s", continuousQuery.project, continuousQuery.tableName);
        return executor.executeQuery(project, prestoQuery).getResult().thenApply(result -> {
            if(result.getError() == null) {
                reportDatabase.createContinuousQuery(continuousQuery);
            }
            return result;
        });
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project) {
        return list(project).stream()
                .map(view -> new Tuple<>(view.tableName, metastore.getCollection(project, view.getTableName())))
                .collect(Collectors.toMap(t -> t.v1(), t -> t.v2()));
    }

    /*
     * Currently, the implementation use time column for incremental computation.
     * However the column needs to indexed because queries that filters a column in a large table without an index is not efficient.
     * After each operation, we drop / re-create index so that the query doesn't spend too much time filtering rows.
     * However; new rows need to indexed so it affects write performance.
     * TODO: find a better way to process rows in batches.
     * Possible ways:
     * 1. Use LISTEN / NOTIFY queue. (- AFAIK LISTEN fetches rows one by one)
     * 1. Use logical decoder introduced in 9.4. (- Needs an additional custom output decoder needs to be installed.)
     */
    private void updateTable() {
        Map<String, List<ContinuousQuery>> allContinuousQueries = reportDatabase.getAllContinuousQueries().stream()
                .collect(Collectors.groupingBy(k -> k.project));

        for (Map.Entry<String, List<String>> entry : metastore.getAllCollections().entrySet()) {
            String project = entry.getKey();

            List<ContinuousQuery> continuousQueries = allContinuousQueries.get(project);

            if(continuousQueries == null) {
                continue;
            }

            for (String collection : entry.getValue()) {
                List<ContinuousQuery> queriesForCollection = continuousQueries.stream()
                        .filter(p -> p.collections.contains(collection)).collect(Collectors.toList());

                if(queriesForCollection.size() == 0){
                    continue;
                }

                String sqlQuery = buildQueryForCollection(project, collection, queriesForCollection);
                executor.executeRawStatement(sqlQuery).getResult().thenAccept(result -> {
                    if(result.isFailed()) {
                        String query = sqlQuery;
                        LOGGER.error("Failed to update continuous query states: {}", result.getError());
                    }
                }).join();
//            String s1 = "CREATE INDEX graph_mv_latest ON graph (xaxis, value) WHERE  ts >= '-infinity';";
            }
        }
    }

    private String buildQueryForCollection(String project, String collection, List<ContinuousQuery> queriesForCollection) {
        StringBuilder builder = new StringBuilder("DO $$\n" +
                "DECLARE newTime int;\n" +
                "DECLARE lastTime int;\n" +
                "DECLARE tableHash bigint;\n" +
                "DECLARE updatedRows record;\n");

        builder.append(format(
                "BEGIN\n" +
                "   SELECT CAST (EXTRACT(epoch FROM now()) AS int4) AS TIME into newTime;\n" +
                "   SELECT last_sync into lastTime FROM collections_last_sync WHERE project = '%1$s' AND collection = '%2$s';\n\n" +
                "   IF lastTime IS NULL THEN\n" +
                "       INSERT INTO collections_last_sync VALUES ('%1$s', '%2$s', newTime)\n" +
                "       RETURNING last_sync into lastTime;\n" +
                "   END IF;\n\n"+
                "   CREATE TEMPORARY TABLE stream ON COMMIT DROP AS (\n" +
                "       SELECT * FROM %1$s.%2$s WHERE TIME BETWEEN lastTime AND newTime\n" +
                "   );\n\n", project, collection));

        for (ContinuousQuery report : queriesForCollection) {

            builder.append(format(
                    "   CREATE TEMPORARY TABLE stream_%s ON COMMIT DROP AS (%s);\n",
                    report.getTableName(), report.query));

            builder.append(format(
                    "   tableHash := ('x'||substr(md5('%s.%s'),1,16))::bit(64)::bigint;\n",
                    report.project, report.getTableName()));

            builder.append(
                    "   PERFORM pg_advisory_lock(tableHash);\n");

            Map<String, PostgresqlFunction> metadata = JsonHelper.convert(report.options.get("_metadata"),
                    new TypeReference<Map<String, PostgresqlFunction>>() {});

            String tableName = report.getTableName();
            String aggFields = metadata.entrySet().stream()
                    .filter(c -> c.getValue() != null)
                    .map(c -> buildUpdateState(tableName, c.getKey(), c.getValue()))
                    .collect(Collectors.joining(", "));

            String groupedWhere = metadata.entrySet().stream()
                    .filter(c -> c.getValue() == null)
                    .map(c -> format("%1$s.\"%2$s\" = stream_%1$s.\"%2$s\"", tableName, c.getKey()))
                    .collect(Collectors.joining(" AND "));
            if(!groupedWhere.isEmpty()) {
                groupedWhere = " WHERE "+groupedWhere;
            }

            String returningFields = metadata.entrySet().stream()
                    .filter(c -> c.getValue() == null)
                    .map(c -> tableName + '.' + '"' + c.getKey() + '"')
                    .collect(Collectors.joining(", "));
            returningFields = returningFields.isEmpty() ? "1": returningFields;

            // It's unfortunate that postgresql doesn't support UPSERT yet. (9.4)
            // Eventually UPDATE rate will be higher then INSERT rate so I think we should optimize UPDATE rather than INSERT
            builder.append(format("   WITH updated AS (UPDATE %s.%s SET %s FROM stream_%s %s RETURNING %s)\n",
                    project, tableName, aggFields, tableName, groupedWhere, returningFields));
            builder.append(format("   INSERT INTO %s.%s (SELECT * FROM stream_%s WHERE NOT EXISTS (SELECT * FROM updated) );\n",
                    project, tableName, tableName));
            builder.append(
                    "   PERFORM pg_advisory_unlock(tableHash);\n\n");
        }

        builder.append(format("   UPDATE collections_last_sync " +
                "SET last_sync = newTime WHERE project = '%s' AND collection = '%s';", project, collection));

        builder.append("\n END$$;");
        return builder.toString();
    }

    private String buildUpdateState(String tableName, String key, PostgresqlFunction value) {
        switch (value) {
            case count:
            case sum:
                return format("\"%2$s\" = %1$s.\"%2$s\" + stream_%1$s.\"%2$s\"", tableName, key);
            case max:
                return format("\"%2$s\" = max(%1$s_update.\"%2$s\", %1$s.\"%2$s\")", tableName, key);
            case min:
                return format("\"%2$s\" = min(%1$s_update.\"%2$s\", %1$s.\"%2$s\")", tableName, key);
            default:
                throw new IllegalStateException();
        }
    }

    private Map<String, PostgresqlFunction> processQuery(ContinuousQuery report) {
        Query statement = (Query) new SqlParser().createStatement(report.query);
        QuerySpecification querySpecification = (QuerySpecification) (statement.getQueryBody());

        if(querySpecification.getSelect().isDistinct())
            throw new IllegalArgumentException("Distinct query is not supported");

        Map<String, PostgresqlFunction> columns = Maps.newHashMap();

        List<SelectItem> selectItems = querySpecification.getSelect().getSelectItems();

        for (int i = 0; i < selectItems.size(); i++) {
            SelectItem selectItem = selectItems.get(i);

            if(selectItem instanceof AllColumns) {
                throw new IllegalArgumentException("Select all (*) is not supported in continuous queries yet. Please specify the columns.");
            }

            if(!(selectItem instanceof SingleColumn)) {
                throw new IllegalArgumentException(format("Column couldn't identified: %s", selectItem));
            }

            SingleColumn selectItem1 = (SingleColumn) selectItem;
            Expression exp = selectItem1.getExpression();
            if(exp instanceof FunctionCall) {
                FunctionCall functionCall = (FunctionCall) exp;
                if (functionCall.isDistinct()) {
                    throw new IllegalArgumentException("Distinct in functions is not supported");
                }

                if (functionCall.getWindow().isPresent()) {
                    throw new IllegalArgumentException("Window is not supported");
                }

                PostgresqlFunction func;
                try {
                    func = PostgresqlFunction.valueOf(functionCall.getName().toString().toLowerCase().trim());
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(format("Unsupported function '%s'." +
                                    "Currently you can use one of these aggregation functions: %s",
                            functionCall.getName(), PostgresqlFunction.values()));
                }
                Optional<String> alias = selectItem1.getAlias();
                if(!alias.isPresent()) {
                    throw new IllegalArgumentException(format("Column '%s' must have an alias", selectItem1));
                }
                columns.put(alias.get(), func);
            }
        }

        for (Expression expression : querySpecification.getGroupBy()) {
            if(expression instanceof LongLiteral) {
                SingleColumn selectItem = (SingleColumn) selectItems.get(Ints.checkedCast(((LongLiteral) expression).getValue())-1);
                Optional<String> alias = selectItem.getAlias();
                if(!selectItem.getAlias().isPresent()) {
                    if(selectItem.getExpression() instanceof QualifiedNameReference) {
                        columns.put(((QualifiedNameReference) selectItem.getExpression()).getName().getSuffix(), null);
                    }else {
                        throw new IllegalArgumentException(format("Column '%s' must have an alias", selectItem));
                    }
                }else {
                    columns.put(alias.get(), null);
                }
            } else {
                throw new IllegalArgumentException("The GROUP BY references must be the column ids. Example: (GROUP BY 1, 2");
            }
        }

        if(selectItems.size() != columns.size()) {
            Object[] unknownColumns = selectItems.stream().filter(item -> {
                Optional<String> alias = ((SingleColumn) item).getAlias();
                return alias.isPresent() && columns.containsKey(alias.get());
            }).toArray();
            throw new IllegalArgumentException(format("Continuous queries must also be aggregation queries." +
                    "These columns are neither aggregation function not GROUP BY column: %s", Arrays.toString(unknownColumns)));
        }

        return columns;
    }


    public static enum PostgresqlFunction {
        // implement approximate_count using hyperloglog algorithm: https://www.periscope.io/blog/hyperloglog-in-pure-sql.html
        count, max, min, sum
    }
}
