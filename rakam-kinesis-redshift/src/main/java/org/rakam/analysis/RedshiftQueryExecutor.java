package org.rakam.analysis;

import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/07/15 19:24.
 */
public class RedshiftQueryExecutor implements QueryExecutor {
    @Override
    public QueryExecution executeQuery(String project, String sqlQuery, int limit) {
        return null;
    }

    @Override
    public QueryExecution executeQuery(String project, String sqlQuery) {
        return null;
    }

    @Override
    public QueryExecution executeStatement(String project, String sqlQuery) {
        return null;
    }
}
