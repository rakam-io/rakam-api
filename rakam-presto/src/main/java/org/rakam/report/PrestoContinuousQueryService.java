package org.rakam.report;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;

import javax.inject.Inject;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.rakam.report.PrestoQueryExecution.fromPrestoType;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/04/15 04:38.
 */
public class PrestoContinuousQueryService extends ContinuousQueryService {
    public final static String PRESTO_STREAMING_CATALOG_NAME = "streaming";
    private final QueryMetadataStore database;
    private final PrestoQueryExecutor executor;
    private final Metastore metastore;

    @Inject
    public PrestoContinuousQueryService(QueryMetadataStore database, PrestoQueryExecutor executor, Metastore metastore) {
        super(database);
        this.database = database;
        this.executor = executor;
        this.metastore = metastore;
    }

    @Override
    public CompletableFuture<QueryResult> create(ContinuousQuery report) {
        Query statement = (Query) new SqlParser().createStatement(report.query);
        statement.accept(new AstVisitor<Void, Void>() {
            @Override
            protected Void visitQualifiedNameReference(QualifiedNameReference node, Void context)
            {
                if(!node.getName().equals("stream")) {
                    throw new IllegalArgumentException("Continuous queries must only reference 'stream' table.");
                }
                return null;
            }

            @Override
            protected Void visitJoin(Join node, Void context)
            {
                throw new IllegalArgumentException("Currently, continuous tables don't support JOINs.");
            }
        }, null);
        database.createContinuousQuery(report);
        return CompletableFuture.completedFuture(QueryResult.empty());
    }

    @Override
    public CompletableFuture<Boolean> delete(String project, String name) {
        ContinuousQuery continuousQuery = database.getContinuousQuery(project, name);

        String prestoQuery = format("drop view %s.%s.%s", PRESTO_STREAMING_CATALOG_NAME,
                continuousQuery.project, continuousQuery.tableName);
        return executor.executeRawQuery(prestoQuery).getResult().thenApply(result -> {
            if(result.getError() == null) {
                database.createContinuousQuery(continuousQuery);
                return true;
            }
            return false;
        });
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project) {
        List<SimpleImmutableEntry<String, CompletableFuture<QueryResult>>> collect = database.getContinuousQueries(project).stream()
                .map(query -> {
                    PrestoQueryExecution prestoQueryExecution = executor.executeRawQuery(format("describe %s.%s.%s",
                            PRESTO_STREAMING_CATALOG_NAME, project, query.tableName));
                    return new SimpleImmutableEntry<>(query.tableName, prestoQueryExecution
                            .getResult());
                }).collect(Collectors.toList());

        CompletableFuture.allOf(collect.stream().map(c -> c.getValue()).toArray(CompletableFuture[]::new)).join();

        return collect.stream().collect(
                Collectors.toMap(
                        item -> item.getKey(),
                        item -> item.getValue().join().getResult()
                            .stream()
                            .map(row -> new SchemaField((String) row.get(0), fromPrestoType((String) row.get(1)), (Boolean) row.get(2)))
                            .collect(Collectors.toList())
                ));

    }
}
