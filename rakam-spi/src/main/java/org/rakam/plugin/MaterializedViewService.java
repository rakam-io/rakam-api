package org.rakam.plugin;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.report.QueryStats;
import org.rakam.util.QueryFormatter;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;


public abstract class MaterializedViewService {
    private final SqlParser parser = new SqlParser();

    protected final QueryMetadataStore database;
    protected final QueryExecutor queryExecutor;
    private final Clock clock;

    public MaterializedViewService(QueryExecutor queryExecutor, QueryMetadataStore database, Clock clock) {
        this.database = database;
        this.queryExecutor = queryExecutor;
        this.clock = clock;
    }

    public CompletableFuture<Void> create(MaterializedView materializedView) {
        materializedView.validateQuery();

        database.createMaterializedView(materializedView);
        return CompletableFuture.completedFuture(null);
    }

    private QueryExecution createTable(MaterializedView materializedView) {
        StringBuilder builder = new StringBuilder();
        Query statement;
        synchronized (parser) {
            statement = (Query) parser.createStatement(materializedView.query);
        }

        new QueryFormatter(builder, a -> queryExecutor.formatTableReference(materializedView.project, a)).process(statement, 1);

        return queryExecutor.executeRawStatement(format("CREATE TABLE %s AS (%s)",
                queryExecutor.formatTableReference(materializedView.project, QualifiedName.of("materialized", materializedView.tableName)),
                builder.toString()));
    }

    public CompletableFuture<QueryResult> delete(String project, String name) {
        MaterializedView materializedView = database.getMaterializedView(project, name);
        database.deleteMaterializedView(project, name);
        String reference = queryExecutor.formatTableReference(materializedView.project, QualifiedName.of("materialized", materializedView.tableName));
        return queryExecutor.executeRawQuery(format("DELETE TABLE %s",
                reference)).getResult();
    }
    public List<MaterializedView> list(String project) {
        return database.getMaterializedViews(project);
    }

    public MaterializedView get(String project, String name) {
        return database.getMaterializedView(project, name);
    }

    public abstract Map<String, List<SchemaField>> getSchemas(String project);

    public QueryExecution lockAndUpdateView(MaterializedView materializedView) {
        CompletableFuture<Boolean> f = new CompletableFuture<>();
        boolean availableForUpdating = database.updateMaterializedView(materializedView, f);
        if(availableForUpdating) {
            String reference = queryExecutor.formatTableReference(materializedView.project, QualifiedName.of("materialized", materializedView.tableName));

            if (materializedView.lastUpdate != null) {
                QueryResult result = queryExecutor.executeRawStatement(format("DROP TABLE %s", reference)).getResult().join();
                if (result.isFailed()) {
                    return new QueryExecution() {
                        @Override
                        public QueryStats currentStats() {
                            return null;
                        }

                        @Override
                        public boolean isFinished() {
                            return true;
                        }

                        @Override
                        public CompletableFuture<QueryResult> getResult() {
                            return CompletableFuture.completedFuture(result);
                        }

                        @Override
                        public String getQuery() {
                            return null;
                        }

                        @Override
                        public void kill() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            }

            QueryExecution queryExecution = createTable(materializedView);

            queryExecution.getResult().thenAccept(result -> {
                f.complete(!result.isFailed());
            });

            return queryExecution;
        }

        return null;
    }}
