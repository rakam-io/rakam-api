package org.rakam.report;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/04/15 00:18.
 */
public interface QueryExecutor {
    QueryExecution executeQuery(String project, String sqlQuery);
    QueryExecution executeStatement(String project, String sqlQuery);
}
