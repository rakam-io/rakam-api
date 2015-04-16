package org.rakam.plugin;

import com.facebook.presto.sql.tree.Statement;
import org.rakam.collection.event.metastore.ReportMetadataStore;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.report.QueryStats;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/04/15 05:30.
 */
public abstract class AbstractReportService {
    final ReportMetadataStore database;
    final QueryExecutor queryExecutor;

    public AbstractReportService(QueryExecutor queryExecutor, ReportMetadataStore database) {
        this.queryExecutor = queryExecutor;
        this.database = database;
    }

    public void create(MaterializedView materializedView) {
        queryExecutor.executeQuery(format("CREATE TABLE _%s AS (%s LIMIT 0)", materializedView.tableName, materializedView.query));
        database.saveMaterializedView(materializedView);
    }

    protected abstract String buildQuery(String project, Statement query);

    public QueryExecution execute(String project, Statement statement) {
        return queryExecutor.executeQuery(buildQuery(project, statement));
    }

    public List<MaterializedView> listMaterializedViews(String project) {
        return database.getMaterializedViews(project);
    }

    public CompletableFuture<? extends QueryResult> deleteMaterializedView(String project, String name) {
        database.deleteMaterializedView(project, name);
        return queryExecutor.executeQuery(format("DELETE TABLE %s", name)).getResult();
    }

    public MaterializedView getMaterializedView(String project, String name) {
        return database.getMaterializedView(project, name);
    }

    public QueryExecution updateMaterializedView(String project, String reportName) {
        MaterializedView materializedView = database.getMaterializedView(project, reportName);
        if(materializedView.lastUpdate!=null) {
            QueryResult result = queryExecutor.executeQuery(format("DROP TABLE %s", materializedView.tableName)).getResult().join();
            if(result.isFailed()) {
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
                    public CompletableFuture<? extends QueryResult> getResult() {
                        return CompletableFuture.completedFuture(result);
                    }

                    @Override
                    public String getQuery() {
                        return null;
                    }
                };
            }
        }
        String sqlQuery = buildQuery(materializedView.project, materializedView.query);
        return queryExecutor.executeQuery(format("CREATE TABLE %s AS (%s)", materializedView.tableName, sqlQuery));
    }
}
