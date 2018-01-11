package org.rakam.clickhouse;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.inject.Inject;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.clickhouse.analysis.ClickHouseQueryExecution;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.report.QuerySampling;
import org.rakam.util.RakamException;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class ClickHouseQueryExecutor implements QueryExecutor {
    private final ClickHouseConfig config;
    private final Metastore metastore;
    private final ProjectConfig projectConfig;

    @Inject
    public ClickHouseQueryExecutor(ProjectConfig projectConfig, ClickHouseConfig config, Metastore metastore) {
        this.projectConfig = projectConfig;
        this.config = config;
        this.metastore = metastore;
    }

    @Override
    public QueryExecution executeRawQuery(String sqlQuery, ZoneId zoneId, Map<String, String> sessionParameters, String apiKey) {
        return new ClickHouseQueryExecution(config, sqlQuery);
    }

    @Override
    public QueryExecution executeRawStatement(String sqlQuery) {
        ClickHouseQueryExecution.runStatement(config, sqlQuery);
        return QueryExecution.completedQueryExecution(sqlQuery, QueryResult.empty());
    }

    @SuppressWarnings("Duplicates")
    @Override
    public String formatTableReference(String project, QualifiedName node, Optional<QuerySampling> sample, Map<String, String> sessionParameters) {
        if (node.getPrefix().isPresent()) {
            String prefix = node.getPrefix().get().toString();
            if (prefix.equals("continuous")) {
                return ".`" + project + "`.`$continuous_" + checkCollection(node.getSuffix(), '`');
            } else if (prefix.equals("materialized")) {
                return ".`" + project + "`.`$materialized_" + checkCollection(node.getSuffix(), '`');
            } else if (prefix.equals("user")) {
                throw new IllegalArgumentException();
            } else if (!prefix.equals("collection")) {
                throw new RakamException("Schema does not exist: " + prefix, BAD_REQUEST);
            }
        }

        // special prefix for all columns
        if (node.getSuffix().equals("_all") && !node.getPrefix().isPresent()) {
            List<Map.Entry<String, List<SchemaField>>> collections = metastore.getCollections(project).entrySet().stream()
                    .filter(c -> !c.getKey().startsWith("_"))
                    .collect(Collectors.toList());
            if (!collections.isEmpty()) {
                String sharedColumns = collections.get(0).getValue().stream()
                        .filter(col -> collections.stream().allMatch(list -> list.getValue().contains(col)))
                        .map(f -> checkTableColumn(f.getName(), '`'))
                        .collect(Collectors.joining(", "));

                return "(" + collections.stream().map(Map.Entry::getKey)
                        .map(collection -> format("select '%s' as `_collection`, %s from %s",
                                collection,
                                sharedColumns.isEmpty() ? "1" : sharedColumns,
                                getTableReference(project, QualifiedName.of(collection))))
                        .collect(Collectors.joining(" union all ")) + ") ";
            } else {
                return String.format("(select '' as `_collection`, '' as _user, now() as %s limit 0)", checkTableColumn(projectConfig.getTimeColumn(), '`'));
            }

        } else {
            return getTableReference(project, node);
        }
    }

    private String getTableReference(String project, QualifiedName node) {
        String hotStoragePrefix = Optional.ofNullable(config.getHotStoragePrefix()).map(e -> e + "_").orElse(null);
        String coldStoragePrefix = Optional.ofNullable(config.getColdStoragePrefix()).map(e -> e + "_").orElse("");
        String table = project + "." + checkCollection(node.getSuffix(), '`');

        if (hotStoragePrefix != null) {
            return "((select * from " + coldStoragePrefix + table + " union all " +
                    "select * from " + hotStoragePrefix + table + ")" +
                    " as " + node.getSuffix() + ")";
        } else {
            return coldStoragePrefix + table;
        }
    }
}
