package org.rakam.presto.analysis;

import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
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

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.String.format;
import static org.rakam.presto.analysis.PrestoQueryExecution.fromPrestoType;

public class PrestoContinuousQueryService extends ContinuousQueryService {

    private final QueryMetadataStore database;
    private final PrestoQueryExecutor executor;
    private final PrestoConfig config;

    @Inject
    public PrestoContinuousQueryService(QueryMetadataStore database, PrestoQueryExecutor executor, PrestoConfig config) {
        super(database);
        this.database = database;
        this.executor = executor;
        this.config = config;
    }

    @Override
    public CompletableFuture<QueryResult> create(ContinuousQuery report, boolean replayHistoricalData) {
        StringBuilder builder = new StringBuilder();

        new QueryFormatter(builder, name ->
                executor.formatTableReference(report.project, name)).process(report.getQuery(), 1);

        String prestoQuery = format("create view %s.\"%s\".\"%s\" as %s", config.getStreamingConnector(),
                report.project, report.tableName, builder.toString());


        PrestoQueryExecution prestoQueryExecution;
        if (!report.partitionKeys.isEmpty()) {
            ImmutableMap<String, String> sessionParameter = ImmutableMap.of(config.getStreamingConnector() + ".partition_keys",
                    Joiner.on("|").join(report.partitionKeys));
            prestoQueryExecution = executor.executeRawQuery(prestoQuery, sessionParameter);
        } else {
            prestoQueryExecution = executor.executeRawQuery(prestoQuery);
        }

        return prestoQueryExecution.getResult().thenApply(result -> {
            if (result.getError() == null) {
                try {
                    database.createContinuousQuery(report);
                } catch (AlreadyExistsException e) {
                    database.deleteContinuousQuery(report.project, report.tableName);
                    database.createContinuousQuery(report);
                }

                if (replayHistoricalData) {
                    return executor.executeRawStatement(format("create or replace view %s.\"%s\".\"%s\" as %s", config.getStreamingConnector(),
                            report.project, report.tableName, builder.toString())).getResult().join();
                }

                return QueryResult.empty();
            } else {
                if (result.getError().message.contains("already exists")) {
                    throw new AlreadyExistsException("Continuous query", HttpResponseStatus.BAD_REQUEST);
                }
            }
            return result;
        });
    }

    @Override
    public CompletableFuture<Boolean> delete(String project, String tableName) {
        String prestoQuery = format("drop table %s.\"%s\".\"%s\"", config.getStreamingConnector(), project, tableName);
        return executor.executeRawQuery(prestoQuery).getResult().thenApply(result -> {
            if (result.getError() == null) {
                database.deleteContinuousQuery(project, tableName);
                return true;
            }
            return false;
        });
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project) {
        QueryResult result = executor.executeRawQuery(String.format("select table_name, column_name, data_type from %s.information_schema.columns \n" +
                        "where table_schema = '%s' order by table_name, ordinal_position",
                config.getStreamingConnector(), project)).getResult().join();

        if (result.isFailed()) {
            throw new RakamException("Error while fetching metadata: "+result.getError().message, INTERNAL_SERVER_ERROR);
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
    public boolean test(String project, String query) {
        return true;
    }

}
