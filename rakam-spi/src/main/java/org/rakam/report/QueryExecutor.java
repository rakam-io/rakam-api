package org.rakam.report;


import com.facebook.presto.sql.tree.QualifiedName;

public interface QueryExecutor {
    QueryExecution executeRawQuery(String sqlQuery);
    QueryExecution executeRawStatement(String sqlQuery);
    String formatTableReference(String project, QualifiedName name);
}
