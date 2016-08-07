package org.rakam.presto.analysis;

import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.QueryFormatter;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static org.rakam.presto.analysis.PrestoQueryExecution.fromPrestoType;

public class PrestoContinuousQueryService
        extends ContinuousQueryService
{

    private final QueryMetadataStore database;
    private final PrestoQueryExecutor executor;
    private final PrestoConfig config;
    private final SqlParser sqlParser = new SqlParser();

    @Inject
    public PrestoContinuousQueryService(QueryMetadataStore database, PrestoQueryExecutor executor, PrestoConfig config)
    {
        super(database);
        this.database = database;
        this.executor = executor;
        this.config = config;
    }

    public String build(String project, Query query)
    {
        StringBuilder builder = new StringBuilder();

        new QueryFormatter(builder, name -> {
            if (name.getSuffix().equals("_all") && name.getPrefix().map(prefix -> prefix.equals("collection")).orElse(true)) {
                return format("_all.%s", project);
            }
            return executor.formatTableReference(project, name);
        }, '"').process(query, 1);

        return builder.toString();
    }

    @Override
    public QueryExecution create(String project, ContinuousQuery report, boolean replayHistoricalData)
    {
        String query = build(project, report.getQuery());
        String prestoQuery = format("create view %s.\"%s\".\"%s\" as %s", config.getStreamingConnector(),
                project, report.tableName, query);

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        PrestoQueryExecution prestoQueryExecution;
        if (!report.partitionKeys.isEmpty()) {
            builder.put(config.getStreamingConnector() + ".partition_keys",
                    Joiner.on("|").join(report.partitionKeys));
        }

        builder.put(config.getStreamingConnector() + ".process_historical_data", Boolean.toString(replayHistoricalData));

        if (report.getOptions() != null && report.getOptions().get("realtime") != null) {
            builder.put(config.getStreamingConnector() + ".window_duration",
                    report.getOptions().get("realtime").toString());
        }

        prestoQueryExecution = executor.executeRawQuery(prestoQuery, builder.build(), config.getStreamingConnector());

        return new DelegateQueryExecution(prestoQueryExecution, result -> {
            if (result.getError() == null) {
                try {
                    database.createContinuousQuery(project, report);
                }
                catch (AlreadyExistsException e) {
                    database.deleteContinuousQuery(project, report.tableName);
                    database.createContinuousQuery(project, report);
                }

                return result;
            }
            else {
                if (result.getError().message.contains("already exists")) {
                    throw new AlreadyExistsException("Continuous query", BAD_REQUEST);
                }

                throw new RakamException("Error while creating continuous query: " + result.getError().message, INTERNAL_SERVER_ERROR);
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> delete(String project, String tableName)
    {
        String prestoQuery = format("drop table %s.\"%s\".\"%s\"", config.getStreamingConnector(), project, tableName);
        return executor.executeRawQuery(prestoQuery).getResult().thenApply(result -> {
            if (result.getError() == null) {
                database.deleteContinuousQuery(project, tableName);
                return true;
            }
            else {
                throw new RakamException("Error while deleting continuous query:" + result.getError().message, BAD_REQUEST);
            }
        });
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project)
    {
        QueryResult result = executor.executeRawQuery(format("select table_name, column_name, data_type from %s.information_schema.columns \n" +
                        "where table_schema = '%s' order by table_name, ordinal_position",
                config.getStreamingConnector(), project)).getResult().join();

        if (result.isFailed()) {
            throw new RakamException("Error while fetching metadata: " + result.getError().message, INTERNAL_SERVER_ERROR);
        }

        HashMap<String, List<SchemaField>> map = new HashMap<>();
        for (List<Object> column : result.getResult()) {
            String tableName = (String) column.get(0);
            String columnName = (String) column.get(1);
            TypeSignature dataType = TypeSignature.parseTypeSignature((String) column.get(2));

            SchemaField field = new SchemaField(columnName, fromPrestoType(dataType.getBase(),
                    dataType.getParameters().stream().map(e -> e.getTypeSignature().getBase()).iterator()));

            map.computeIfAbsent(tableName, (key) -> new ArrayList<>()).add(field);
        }

        return map;
    }

    @Override
    public synchronized boolean test(String project, String query)
    {
        Statement statement = sqlParser.createStatement(query);
        if (!(statement instanceof Query)) {
            throw new RakamException("The query must be SELECT.", BAD_REQUEST);
        }

        QueryBody queryBody = ((Query) statement).getQueryBody();
        if (!(queryBody instanceof QuerySpecification)) {
            throw new RakamException("The query be in a form of SELECT col FROM.", BAD_REQUEST);
        }

        List<SelectItem> selectItems = ((QuerySpecification) queryBody).getSelect().getSelectItems();

        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof AllColumns) {
                throw new RakamException("Wildcard in SELECT is not supported.", BAD_REQUEST);
            }

            if (selectItem instanceof SingleColumn) {
                SingleColumn singleColumn = (SingleColumn) selectItem;
                if (!singleColumn.getAlias().isPresent() && !(singleColumn.getExpression() instanceof QualifiedNameReference)) {
                    throw new RakamException(format("Alias for %s is missing", singleColumn.toString()), BAD_REQUEST);
                }

                continue;
            }

            throw new IllegalStateException();
        }

        return true;
    }

    @Override
    public QueryExecution refresh(String project, String tableName)
    {
        ContinuousQuery continuousQuery = get(project, tableName);
        String query = build(project, continuousQuery.getQuery());

        return executor.executeRawQuery(format("create or replace view %s.\"%s\".\"%s\" as %s", config.getStreamingConnector(),
                project, tableName, query), ImmutableMap.of(config.getStreamingConnector() + ".append_data", "false"), config.getStreamingConnector());
    }
}
