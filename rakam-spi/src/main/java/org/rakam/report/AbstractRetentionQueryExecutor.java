package org.rakam.report;

import com.facebook.presto.sql.tree.Expression;
import com.google.common.primitives.Ints;
import org.rakam.analysis.CalculatedUserSet;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.analysis.metadata.Metastore;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.RakamSqlFormatter.formatExpression;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.*;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public abstract class AbstractRetentionQueryExecutor implements RetentionQueryExecutor {
    private final int MAXIMUM_LEAD = 15;
    private final QueryExecutorService executor;
    private final static String CONNECTOR_FIELD = "_user";
    private final Metastore metastore;
    private final MaterializedViewService materializedViewService;
    private final ContinuousQueryService continuousQueryService;

    public AbstractRetentionQueryExecutor(QueryExecutorService executor,
                                          Metastore metastore,
                                          MaterializedViewService materializedViewService,
                                          ContinuousQueryService continuousQueryService) {
        this.executor = executor;
        this.metastore = metastore;
        this.materializedViewService = materializedViewService;
        this.continuousQueryService = continuousQueryService;
    }

    public abstract String diffTimestamps(DateUnit dateUnit, String start, String end);


    @Override
    public QueryExecution query(String project, Optional<RetentionAction> firstAction, Optional<RetentionAction> returningAction, DateUnit dateUnit, Optional<String> dimension, LocalDate startDate, LocalDate endDate) {
        String timeColumn;

        checkTableColumn(CONNECTOR_FIELD, "connector field");

        if (dateUnit == DAY) {
            timeColumn = "cast(%s as date)";
        } else if (dateUnit == WEEK) {
            timeColumn = "cast(date_trunc('week', %s) as date)";
        } else if (dateUnit == MONTH) {
            timeColumn = "cast(date_trunc('month', %s) as date)";
        } else {
            throw new UnsupportedOperationException();
        }

        LocalDate start;
        LocalDate end;
        if (dateUnit == MONTH) {
            start = startDate.withDayOfMonth(1);
            end = endDate.withDayOfMonth(1).plus(1, ChronoUnit.MONTHS);
        } else if (dateUnit == WEEK) {
            TemporalField fieldUS = WeekFields.of(Locale.US).dayOfWeek();
            start = startDate.with(fieldUS, 1);
            end = endDate.with(fieldUS, 1).plus(1, ChronoUnit.MONTHS);
        } else if (dateUnit == DAY) {
            start = startDate;
            end = endDate;
        } else {
            throw new IllegalStateException();
        }
        int range = Math.min(MAXIMUM_LEAD, Ints.checkedCast(dateUnit.getTemporalUnit().between(start, end)));

        if (range < 0) {
            throw new IllegalArgumentException("startDate must be before endDate.");
        }

        if (range == 0) {
            return QueryExecution.completedQueryExecution(null, QueryResult.empty());
        }

        Set<CalculatedUserSet> missingPreComputedTables = new HashSet<>();

        String firstActionQuery = generateQuery(project, firstAction, CONNECTOR_FIELD, timeColumn, dimension,
                startDate, endDate, missingPreComputedTables);
        String returningActionQuery = generateQuery(project, returningAction, CONNECTOR_FIELD, timeColumn, dimension,
                startDate, endDate, missingPreComputedTables);

        String timeSubtraction = diffTimestamps(dateUnit, "data.date", "returning_action.date") + "-1";

        String dimensionColumn = dimension.isPresent() ? "data.dimension" : "data.date";

        String query = format("with first_action as (\n" +
                        "  %s\n" +
                        "), \n" +
                        "returning_action as (\n" +
                        "  %s\n" +
                        ") \n" +
                        "select %s, cast(null as bigint) as lead, count(*) count from first_action data group by 1 union all\n" +
                        "select %s, %s, count(*) \n" +
                        "from first_action data join returning_action on (data.date < returning_action.date AND data.%s = returning_action.%s) \n" +
                        "where %s < %d group by 1, 2 ORDER BY 1, 2 NULLS FIRST",
                firstActionQuery, returningActionQuery, dimensionColumn,
                dimensionColumn, timeSubtraction, CONNECTOR_FIELD, CONNECTOR_FIELD,
                timeSubtraction, MAXIMUM_LEAD);

        return new DelegateQueryExecution(executor.executeQuery(project, query),
                result -> {
                    result.setProperty("calculatedUserSets", missingPreComputedTables);
                    return result;
                });
    }

    private String generateQuery(String project,
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

            return String.format("select * from (%s) group by 1,2 %s", metastore.getCollectionNames(project).stream()
                    .map(collection -> getTableSubQuery(collection, connectorField,
                            timeColumn, dimension, timePredicate, Optional.empty()))
                    .collect(Collectors.joining(" union all ")), dimension.isPresent() ? ", 3" : "");
        } else {
            String collection = retentionAction.get().collection();

            Optional<String> preComputedTable = getPreComputedTable(project, timePredicate, timeColumn, Optional.of(collection), dimension,
                    retentionAction.get().filter(), missingPreComputedTables, dimension.isPresent());

            if (preComputedTable.isPresent()) {
                return preComputedTable.get();
            }

            return String.format("select * from (%s) group by 1,2 %s", getTableSubQuery(collection, connectorField,
                    timeColumn, dimension, timePredicate, retentionAction.get().filter()), dimension.isPresent() ? ", 3" : "");
        }
    }

    private Optional<String> getPreComputedTable(String project, String timePredicate, String timeColumn, Optional<String> collection, Optional<String> dimension,
                                                 Optional<Expression> filter, Set<CalculatedUserSet> missingPreComputedTables,
                                                 boolean dimensionRequired) {
        String tableName = "_users_daily" + collection.map(value -> "_" + value).orElse("") + dimension.map(value -> "_by_" + value).orElse("");

        if (filter.isPresent()) {
            try {
                String preComputedTablePrefix = tableName + "_by_";
                return Optional.of(filter.get().accept(new PreComputedTableSubQueryVisitor(columnName -> {
                    if (continuousQueryService.list(project).stream().anyMatch(e -> e.tableName.equals(preComputedTablePrefix + columnName))) {
                        return Optional.of("continuous." + preComputedTablePrefix + columnName);
                    } else if (materializedViewService.list(project).stream().anyMatch(e -> e.tableName.equals(preComputedTablePrefix + columnName))) {
                        return Optional.of("materialized." + preComputedTablePrefix + columnName);
                    }

                    missingPreComputedTables.add(new CalculatedUserSet(collection, Optional.of(columnName)));
                    return Optional.empty();
                }), false));
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
        return String.format("select %s as date, %s _user from %s where date %s",
                String.format(timeColumn, "date"),
                dimensionRequired ? "dimension, " : "",
                schema + "." + tableNameSuffix.orElse(""), timePredicate);
    }

    private String getTableSubQuery(String collection, String connectorField, String timeColumn, Optional<String> dimension, String timePredicate, Optional<Expression> filter) {
        return format("select %s, %s as date %s from %s where _time %s %s",
                connectorField,
                String.format(timeColumn, "_time"),
                dimension.isPresent() ? ", " + checkTableColumn(dimension.get(), "dimension") + " as dimension" : "",
                collection,
                timePredicate,
                filter.isPresent() ? "and " + formatExpression(filter.get(), reference -> {
                    throw new UnsupportedOperationException();
                }) : "");
    }

}
