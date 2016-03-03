package org.rakam.report;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.primitives.Ints;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.analysis.metadata.Metastore;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.RakamSqlFormatter.formatExpression;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.*;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public abstract class AbstractRetentionQueryExecutor implements RetentionQueryExecutor {
    private final int MAXIMUM_LEAD = 15;
    private final QueryExecutor executor;
    private final static String CONNECTOR_FIELD = "_user";
    private final Metastore metastore;
    private final MaterializedViewService materializedViewService;
    private final ContinuousQueryService continuousQueryService;

    public AbstractRetentionQueryExecutor(QueryExecutor executor, Metastore metastore,
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
            timeColumn = format("cast(_time as date)");
        } else if (dateUnit == WEEK) {
            timeColumn = format("cast(date_trunc('week', _time) as date)");
        } else if (dateUnit == MONTH) {
            timeColumn = format("cast(date_trunc('month', _time) as date)");
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

        String firstActionQuery = generateQuery(project, firstAction, CONNECTOR_FIELD, timeColumn, dimension, startDate, endDate);
        String returningActionQuery = generateQuery(project, returningAction, CONNECTOR_FIELD, timeColumn, dimension, startDate, endDate);

        String timeSubtraction = diffTimestamps(dateUnit, "data.time", "returning_action.time") + "-1";

        String dimensionColumn = dimension.isPresent() ? "data.dimension" : "data.time";

        String query = format("with first_action as (\n" +
                        "  %s\n" +
                        "), \n" +
                        "returning_action as (\n" +
                        "  %s\n" +
                        ") \n" +
                        "select %s, cast(null as bigint) as lead, count(*) count from first_action data group by 1 union all\n" +
                        "select %s, %s, count(*) \n" +
                        "from first_action data join returning_action on (data.time < returning_action.time AND data.%s = returning_action.%s) \n" +
                        "where %s < %d group by 1, 2 ORDER BY 1, 2 NULLS FIRST",
                firstActionQuery, returningActionQuery, dimensionColumn,
                dimensionColumn, timeSubtraction, CONNECTOR_FIELD, CONNECTOR_FIELD,
                timeSubtraction, MAXIMUM_LEAD);

        return executor.executeRawQuery(query);
    }

    private String generateQuery(String project,
                                 Optional<RetentionAction> retentionAction,
                                 String connectorField,
                                 String timeColumn,
                                 Optional<String> dimension,
                                 LocalDate startDate,
                                 LocalDate endDate) {

        String timePredicate = String.format("between date '%s' and date '%s' + interval '1' day",
                startDate.format(ISO_LOCAL_DATE), endDate.format(ISO_LOCAL_DATE));

        if (!retentionAction.isPresent()) {
            Optional<String> preComputedTable = getPreComputedTable(project, timePredicate, dimension);

            if (preComputedTable.isPresent()) {
                return preComputedTable.get();
            }

            return metastore.getCollectionNames(project).stream()
                    .map(collection -> getTableSubQuery(project, collection, connectorField,
                            timeColumn, dimension, timePredicate, Optional.empty()))
                    .collect(Collectors.joining(" union all "));
        } else {
            String collection = retentionAction.get().collection();

            Optional<String> preComputedTable = getPreComputedTable(project, timePredicate,
                    dimension.isPresent() ? dimension.map(dim -> collection + "_" + dim) : Optional.of(collection));

            if (preComputedTable.isPresent()) {
                return preComputedTable.get();
            }

            return getTableSubQuery(project, collection, connectorField,
                    timeColumn, dimension, timePredicate, retentionAction.get().filter());
        }
    }

    private Optional<String> getPreComputedTable(String project, String timePredicate, Optional<String> suffix) {
        String tableName = "_users_daily" + (suffix.isPresent() ? "_" + suffix.get() : "");

        if (continuousQueryService.list(project).stream().anyMatch(e -> e.tableName.equals(tableName))) {
            return Optional.of(generatePreCalculatedTableSql(project, suffix, "continuous", timePredicate));
        } else if (materializedViewService.list(project).stream().anyMatch(e -> e.tableName.equals(tableName))) {
            return Optional.of(generatePreCalculatedTableSql(project, suffix, "materialized", timePredicate));
        }

        return Optional.empty();
    }

    private String generatePreCalculatedTableSql(String project, Optional<String> tableNameSuffix, String schema, String timePredicate) {
        return String.format("select date as time, _user from %s where date %s",
                executor.formatTableReference(project, QualifiedName.of(schema,
                        tableNameSuffix.isPresent() ? "_users_daily_" + tableNameSuffix : "_users_daily")), timePredicate);
    }

    private String getTableSubQuery(String project, String collection, String connectorField, String timeColumn, Optional<String> dimension, String timePredicate, Optional<Expression> filter) {
        return format("select %s, %s as time %s from %s where _time %s %s group by 1, 2 %s",
                connectorField,
                timeColumn,
                dimension.isPresent() ? ", " + checkTableColumn(dimension.get(), "dimension") + " as dimension" : "",
                executor.formatTableReference(project, QualifiedName.of(collection)),
                timePredicate,
                filter.isPresent() ? "and " + formatExpression(filter.get(), reference -> executor.formatTableReference(project, reference)) : "",
                dimension.isPresent() ? ", 3" : "");
    }

}
