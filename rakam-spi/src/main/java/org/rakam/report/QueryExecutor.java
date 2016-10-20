package org.rakam.report;

import com.facebook.presto.sql.tree.QualifiedName;

import java.util.Map;
import java.util.Optional;

public interface QueryExecutor
{
    QueryExecution executeRawQuery(String sqlQuery);

    default QueryExecution executeRawQuery(String sqlQuery, Map<String, String> sessionParameters) {
        return executeRawQuery(sqlQuery);
    }

    QueryExecution executeRawStatement(String sqlQuery);

    String formatTableReference(String project, QualifiedName name, Optional<QuerySampling> sample);

    default String formatTableReference(String project, QualifiedName name, Optional<QuerySampling> sample, Map<String, String> sessionParameters)  {
        return formatTableReference(project, name, sample);
    }
}
