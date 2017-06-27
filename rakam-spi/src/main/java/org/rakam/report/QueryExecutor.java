package org.rakam.report;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableMap;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

public interface QueryExecutor
{
    QueryExecution executeRawQuery(String sqlQuery, ZoneId timezone, Map<String, String> sessionParameters);

    default QueryExecution executeRawQuery(String sqlQuery, ZoneId zoneId)
    {
        return executeRawQuery(sqlQuery, zoneId, ImmutableMap.of());
    }

    default QueryExecution executeRawQuery(String sqlQuery)
    {
        return executeRawQuery(sqlQuery, ZoneOffset.UTC, ImmutableMap.of());
    }

    default QueryExecution executeRawQuery(String sqlQuery, Map<String, String> sessionParameters)
    {
        return executeRawQuery(sqlQuery, ZoneOffset.UTC, sessionParameters);
    }

    default QueryExecution executeRawStatement(String sqlQuery, Map<String, String> sessionParameters)
    {
        return executeRawStatement(sqlQuery);
    }

    default QueryExecution executeRawStatement(String sqlQuery) {
        return executeRawQuery(sqlQuery);
    }

    String formatTableReference(String project, QualifiedName name, Optional<QuerySampling> sample, Map<String, String> sessionParameters, String defaultSchema);
}
