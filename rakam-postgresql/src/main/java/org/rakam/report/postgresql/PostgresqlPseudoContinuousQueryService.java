package org.rakam.report.postgresql;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.inject.Inject;
import org.rakam.analysis.postgresql.PostgresqlMetastore;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.report.QueryResult;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class PostgresqlPseudoContinuousQueryService extends ContinuousQueryService {
    private final PostgresqlQueryExecutor executor;
    private final PostgresqlMetastore metastore;
    private final SqlParser parser = new SqlParser();

    @Inject
    public PostgresqlPseudoContinuousQueryService(QueryMetadataStore database, PostgresqlQueryExecutor executor, PostgresqlMetastore metastore) {
        super(database);
        this.executor = executor;
        this.metastore = metastore;
    }

    @Override
    public CompletableFuture<QueryResult> create(ContinuousQuery report) {
//        return executor.executeRawQuery(
//                String.format("CREATE VIEW %s.%s AS %s", report.project, PostgresqlQueryExecutor.CONTINUOUS_QUERY_PREFIX + report.tableName, report.collections))
//                .getResult();
        database.createContinuousQuery(report);
        return CompletableFuture.completedFuture(QueryResult.empty());
    }

    @Override
    public CompletableFuture<Boolean> delete(String project, String name) {
        return executor.executeRawQuery(String.format("DROP VIEW %s.%s", project, name)).getResult().thenApply(result -> !result.isFailed());
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project) {
        return database.getContinuousQueries(project).stream()
                .map(c -> new SimpleImmutableEntry<>(c, executor.executeRawQuery("select * from "+executor.formatTableReference(project, QualifiedName.of("continuous", c.tableName)) + " limit 0")))
                .collect(Collectors.toMap(entry -> entry.getKey().tableName, entry -> entry.getValue().getResult().join().getMetadata()));
    }
}
