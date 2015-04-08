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

    public QueryExecution create(Report report) {
        String sqlQuery = buildQuery(report.project, report.query);
        QueryExecution prestoQuery = queryExecutor.executeQuery(format("CREATE TABLE %s AS (%s)", report.tableName, sqlQuery));
        prestoQuery.getResult().thenAccept(result -> {
            if(!result.isFailed()) {
                database.saveReport(report);
            }
        });
        return prestoQuery;
    }

    protected abstract String buildQuery(String project, Statement query);

    public QueryExecution execute(String project, Statement statement) {
        return queryExecutor.executeQuery(buildQuery(project, statement));
    }

    public List<Report> list(String project) {
        return database.getReports(project);
    }

    public CompletableFuture<? extends QueryResult> delete(String project, String name) {
        database.deleteReport(project, name);
        return queryExecutor.executeQuery(format("DELETE TABLE %s", name)).getResult();
    }

    public Report getReport(String project, String name) {
        return database.getReport(project, name);
    }

    public QueryExecution update(String project, String reportName) {
        Report report = database.getReport(project, reportName);
        QueryResult result = queryExecutor.executeQuery(format("DROP TABLE %s", report.tableName)).getResult().join();
        if (result.getError() != null) {
            String sqlQuery = buildQuery(report.project, report.query);
            return queryExecutor.executeQuery(format("CREATE TABLE %s AS (%s)", report.tableName, sqlQuery));
        } else {
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
}
