package org.rakam.report;


public interface QueryExecutor {
    QueryExecution executeQuery(String project, String sqlQuery, int limit);
    QueryExecution executeQuery(String project, String sqlQuery);
    QueryExecution executeRawQuery(String sqlQuery);
    QueryExecution executeStatement(String project, String sqlQuery);
}
