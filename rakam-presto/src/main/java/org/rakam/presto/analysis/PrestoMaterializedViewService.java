package org.rakam.presto.analysis;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.util.QueryFormatter;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;

public class PrestoMaterializedViewService extends MaterializedViewService {
    public final static String MATERIALIZED_VIEW_PREFIX = "_materialized_";
    public final static SqlParser sqlParser = new SqlParser();

    protected final QueryMetadataStore database;
    protected final QueryExecutor queryExecutor;

    @Inject
    public PrestoMaterializedViewService(QueryExecutor queryExecutor, QueryMetadataStore database, Clock clock) {
        super(database, queryExecutor, clock);
        this.database = database;
        this.queryExecutor = queryExecutor;
    }

    @Override
    public CompletableFuture<Void> create(MaterializedView materializedView) {
        sqlParser.createStatement(materializedView.query).accept(new DefaultTraversalVisitor<Void, Void>() {
            @Override
            protected Void visitSelect(Select node, Void context) {
                for (SelectItem selectItem : node.getSelectItems()) {
                    if(selectItem instanceof AllColumns) {
                        throw new RakamException("Wildcard in select items is not supported in materialized views.", BAD_REQUEST);
                    }
                    if(selectItem instanceof SingleColumn) {
                        SingleColumn selectColumn = (SingleColumn) selectItem;
                        if(!selectColumn.getAlias().isPresent() && !(selectColumn.getExpression() instanceof QualifiedNameReference)) {
                            throw new RakamException(String.format("Column '%s' must have alias", selectColumn.getExpression().toString()), BAD_REQUEST);
                        } else {
                            continue;
                        }
                    }

                    throw new IllegalStateException();
                }
                return null;
            }
        }, null);
        return metadata(materializedView.project, materializedView.query)
                .thenAccept(metadata -> database.createMaterializedView(materializedView));
    }

    private QueryExecution update(MaterializedView materializedView) {
        StringBuilder builder = new StringBuilder();
        Query statement;
        synchronized (sqlParser) {
            statement = (Query) sqlParser.createStatement(materializedView.query);
        }

        new QueryFormatter(builder, a -> queryExecutor.formatTableReference(materializedView.project, a)).process(statement, 1);

        String query;

        if(materializedView.incrementalField == null || materializedView.lastUpdate == null) {
            query = format("CREATE TABLE %s AS (%s) WITH (temporal_column = '_time')",
                    queryExecutor.formatTableReference(materializedView.project, QualifiedName.of("materialized", materializedView.tableName)),
                    builder.toString());
        } else {
            query = format("INSERT INTO %s SELECT * FROM (%s) WHERE %s > from_unixtime(%d)",
                    queryExecutor.formatTableReference(materializedView.project, QualifiedName.of("materialized", materializedView.tableName)),
                    builder.toString(),
                    materializedView.incrementalField,
                    materializedView.lastUpdate.getEpochSecond());
        }


        return queryExecutor.executeRawStatement(query);
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

    public MaterializedView get(String project, String tableName) {
        return database.getMaterializedView(project, tableName);
    }


    public QueryExecution lockAndUpdateView(MaterializedView materializedView) {
        CompletableFuture<Boolean> f = new CompletableFuture<>();
        boolean availableForUpdating = database.updateMaterializedView(materializedView, f);
        if(availableForUpdating) {
            String reference = queryExecutor.formatTableReference(materializedView.project, QualifiedName.of("materialized", materializedView.tableName));

            if (materializedView.lastUpdate != null) {
                QueryExecution execution;
                if(materializedView.incrementalField == null) {
                    execution = queryExecutor.executeRawStatement(format("DROP TABLE %s", reference));
                } else {
                    execution = queryExecutor.executeRawStatement(String.format("DELETE FROM %s WHERE %s > from_unixtime(%d)",
                            reference, materializedView.incrementalField, materializedView.lastUpdate.getEpochSecond()));
                }

                QueryResult result = execution.getResult().join();
                if (result.isFailed()) {
                    return execution;
                }
            }

            QueryExecution queryExecution = update(materializedView);

            queryExecution.getResult().thenAccept(result -> f.complete(!result.isFailed()));

            return queryExecution;
        }

        return null;
    }
}
