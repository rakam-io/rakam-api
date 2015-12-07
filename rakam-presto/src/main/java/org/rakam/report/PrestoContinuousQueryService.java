package org.rakam.report;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.util.QueryFormatter;

import javax.inject.Inject;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;

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
        report.query.accept(new AstVisitor<Void, Void>() {
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
                    PrestoQueryExecution prestoQueryExecution = executor.executeRawQuery(format("select * from %s.%s.%s limit 0",
                            PRESTO_STREAMING_CATALOG_NAME, project, query.tableName));
                    return new SimpleImmutableEntry<>(query.tableName, prestoQueryExecution
                            .getResult());
                }).collect(Collectors.toList());

        CompletableFuture.allOf(collect.stream().map(c -> c.getValue()).toArray(CompletableFuture[]::new)).join();

        ImmutableMap.Builder<String, List<SchemaField>> builder = ImmutableMap.builder();
        for (SimpleImmutableEntry<String, CompletableFuture<QueryResult>> entry : collect) {
            builder.put(entry.getKey(), entry.getValue().join().getMetadata());
        }
        return builder.build();
    }

    @Override
    public List<SchemaField> test(String project, String query) {
        ContinuousQuery continuousQuery = new ContinuousQuery(project, "test", "test", query, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of());

        StringBuilder builder = new StringBuilder();
        new QueryFormatter(builder, qualifiedName -> {
            if(!qualifiedName.getPrefix().isPresent() && qualifiedName.getSuffix().equals("stream")) {
                return replaceStream(continuousQuery);
            }
            return executor.formatTableReference(project, qualifiedName);
        }).process(continuousQuery.query, 1);

        QueryExecution execution = executor
                .executeRawQuery(builder.toString() + " limit 0");
        return execution.getResult().join().getMetadata();
    }

    private String replaceStream(ContinuousQuery report) {

        if (report.collections != null && report.collections.size() == 1) {
            return report.project + "." + report.collections.get(0);
        }

        final List<Map.Entry<String, List<SchemaField>>> collect = metastore.getCollections(report.project)
                .entrySet().stream()
                .filter(c -> report.collections == null || report.collections.contains(c.getKey()))
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
