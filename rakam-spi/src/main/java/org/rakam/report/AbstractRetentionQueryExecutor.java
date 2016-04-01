package org.rakam.report;

import com.facebook.presto.sql.tree.Expression;
import org.rakam.analysis.CalculatedUserSet;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.RakamSqlFormatter.formatExpression;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public abstract class AbstractRetentionQueryExecutor implements RetentionQueryExecutor {
    private final Metastore metastore;
    private final MaterializedViewService materializedViewService;
    private final ContinuousQueryService continuousQueryService;

    public AbstractRetentionQueryExecutor(Metastore metastore,
                                          MaterializedViewService materializedViewService,
                                          ContinuousQueryService continuousQueryService) {
        this.metastore = metastore;
        this.materializedViewService = materializedViewService;
        this.continuousQueryService = continuousQueryService;
    }

    protected String generateQuery(String project,
                                   Optional<RetentionAction> retentionAction,
                                   String connectorField,
                                   String timeColumn,
                                   Optional<String> dimension,
                                   LocalDate startDate,
                                   LocalDate endDate,
                                   Set<CalculatedUserSet> missingPreComputedTables) {

        String timePredicate = String.format("between date '%s' and date '%s' + interval '1' day",
                startDate.format(ISO_LOCAL_DATE), endDate.format(ISO_LOCAL_DATE));

        if (!retentionAction.isPresent()) {
            Optional<String> preComputedTable = getPreComputedTable(project, timePredicate, timeColumn, Optional.empty(),
                    dimension, Optional.empty(), missingPreComputedTables, dimension.isPresent());

            if (preComputedTable.isPresent()) {
                return preComputedTable.get();
            }

            Map<String, List<SchemaField>> collections = metastore.getCollections(project);
            if (collections.size() == 0) {
                return null;
            }

            return String.format("select date, %s set(%s) as %s_set from (%s) group by 1 %s",
                    dimension.map(v -> "dimension, ").orElse(""), connectorField, connectorField,
                    collections.entrySet().stream()
                            .filter(entry -> entry.getValue().stream().anyMatch(e -> e.getName().equals("_user")))
                            .map(collection -> getTableSubQuery(collection.getKey(), connectorField,
                                    timeColumn, dimension, timePredicate, Optional.empty()))
                            .collect(Collectors.joining(" union all ")), dimension.isPresent() ? ", 2" : "");
        } else {
            String collection = retentionAction.get().collection();

            Optional<String> preComputedTable = getPreComputedTable(project, timePredicate, timeColumn, Optional.of(collection), dimension,
                    retentionAction.get().filter(), missingPreComputedTables, dimension.isPresent());

            if (preComputedTable.isPresent()) {
                return preComputedTable.get();
            }

            return String.format("select date, %s set(%s) as %s_set from (%s) group by 1 %s",
                    dimension.map(v -> "dimension, ").orElse(""), connectorField, connectorField,
                    getTableSubQuery(collection, connectorField,
                            timeColumn, dimension, timePredicate, retentionAction.get().filter()), dimension.isPresent() ? ", 2" : "");
        }
    }

    private Optional<String> getPreComputedTable(String project, String timePredicate, String timeColumn, Optional<String> collection, Optional<String> dimension,
                                                 Optional<Expression> filter, Set<CalculatedUserSet> missingPreComputedTables,
                                                 boolean dimensionRequired) {
        String tableName = "_users_daily" + collection.map(value -> "_" + value).orElse("") + dimension.map(value -> "_by_" + value).orElse("");

        if (filter.isPresent()) {
            try {
                String preComputedTablePrefix = tableName + "_by_";
                return Optional.of(new PreComputedTableSubQueryVisitor(columnName -> {
                    if (continuousQueryService.list(project).stream().anyMatch(e -> e.tableName.equals(preComputedTablePrefix + columnName))) {
                        return Optional.of("continuous." + preComputedTablePrefix + columnName);
                    } else if (materializedViewService.list(project).stream().anyMatch(e -> e.tableName.equals(preComputedTablePrefix + columnName))) {
                        return Optional.of("materialized." + preComputedTablePrefix + columnName);
                    }

                    missingPreComputedTables.add(new CalculatedUserSet(collection, Optional.of(columnName)));
                    return Optional.empty();
                }).process(filter.get(), false));
            } catch (UnsupportedOperationException e) {
                return Optional.empty();
            }
        }

        if (continuousQueryService.list(project).stream().anyMatch(e -> e.tableName.equals(tableName))) {
            return Optional.of(generatePreCalculatedTableSql(Optional.of(tableName), "continuous", timePredicate, timeColumn, dimensionRequired));
        } else if (materializedViewService.list(project).stream().anyMatch(e -> e.tableName.equals(tableName))) {
            return Optional.of(generatePreCalculatedTableSql(Optional.of(tableName), "materialized", timePredicate, timeColumn, dimensionRequired));
        }

        missingPreComputedTables.add(new CalculatedUserSet(collection, dimension));
        return Optional.empty();
    }

    private String generatePreCalculatedTableSql(Optional<String> tableNameSuffix, String schema, String timePredicate, String timeColumn, boolean dimensionRequired) {
        return String.format("select %s as date, %s _user_set from %s where date %s",
                String.format(timeColumn, "date"),
                dimensionRequired ? "dimension, " : "",
                schema + "." + tableNameSuffix.orElse(""), timePredicate);
    }

    private String getTableSubQuery(String collection, String connectorField, String timeColumn, Optional<String> dimension, String timePredicate, Optional<Expression> filter) {
        return format("select %s as date, %s %s from %s where _time %s %s",
                String.format(timeColumn, "_time"),
                dimension.isPresent() ? checkTableColumn(dimension.get(), "dimension") + " as dimension, " : "",
                connectorField,
                collection,
                timePredicate,
                filter.isPresent() ? "and " + formatExpression(filter.get(), reference -> {
                    throw new UnsupportedOperationException();
                }) : "",
                dimension.map(v -> ",2").orElse(""));
    }

}
