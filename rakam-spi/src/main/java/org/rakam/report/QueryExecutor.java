package org.rakam.report;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.RequestContext;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

public interface QueryExecutor {
    QueryExecution executeRawQuery(RequestContext context, String sqlQuery, ZoneId timezone, Map<String, String> sessionParameters);

    String formatTableReference(String project, QualifiedName name, Optional<QuerySampling> sample, Map<String, String> sessionParameters);

    default QueryExecution executeRawQuery(RequestContext context, String sqlQuery, ZoneId zoneId) {
        return executeRawQuery(context, sqlQuery, zoneId, ImmutableMap.of());
    }

    default QueryExecution executeRawQuery(RequestContext context, String sqlQuery) {
        return executeRawQuery(context, sqlQuery, ZoneOffset.UTC, ImmutableMap.of());
    }

    default QueryExecution executeRawQuery(RequestContext context, String sqlQuery, Map<String, String> sessionParameters) {
        return executeRawQuery(context, sqlQuery, ZoneOffset.UTC, sessionParameters);
    }

    default QueryExecution executeRawStatement(RequestContext context, String sqlQuery, Map<String, String> sessionParameters) {
        return executeRawQuery(context, sqlQuery, sessionParameters);
    }

    default QueryExecution executeRawStatement(RequestContext context, String sqlQuery) {
        return executeRawQuery(context, sqlQuery);
    }

    default QueryExecution executeRawStatement(String sqlQuery) {
        return executeRawQuery(new RequestContext(null, null), sqlQuery);
    }

}
