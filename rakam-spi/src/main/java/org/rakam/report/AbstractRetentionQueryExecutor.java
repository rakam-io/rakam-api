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

        String firstActionQuery = firstAction.isPresent() ?
                generateQuery(project, firstAction.get().collection(), CONNECTOR_FIELD, timeColumn, dimension,
                        firstAction.get().filter(), startDate, endDate) :
                metastore.getCollectionNames(project).stream()
                        .map(collection -> generateQuery(project, collection, CONNECTOR_FIELD,
                                timeColumn, dimension, Optional.empty(), startDate, endDate))
                        .collect(Collectors.joining(" union all "));

        String returningActionQuery = returningAction.isPresent() ?
                generateQuery(project, returningAction.get().collection(), CONNECTOR_FIELD, timeColumn, dimension,
                        returningAction.get().filter(), startDate, endDate) :
                metastore.getCollectionNames(project).stream()
                        .map(collection -> generateQuery(project, collection, CONNECTOR_FIELD,
                                timeColumn, dimension, Optional.empty(), startDate, endDate))
                        .collect(Collectors.joining(" union all "));

        String query;
        String timeSubtraction = diffTimestamps(dateUnit, "data.time", "returning_action.time") + "-1";

        String dimensionColumn = dimension.isPresent() ? "data.dimension" : "data.time";

        query = format("with first_action as (\n" +
                        "  %s\n" +
                        "), \n" +
                        "returning_action as (\n" +
                        "  %s\n" +
                        ") \n" +
                        "select %s, cast(null as bigint) as lead, count(*) count from first_action data group by 1 union all\n" +
                        "select %s, %s, count(*) \n" +
                        "from first_action data join returning_action on (data.%s = returning_action.%s AND data.time < returning_action.time) \n" +
                        "where %s < %d group by 1, 2 ORDER BY 1, 2 NULLS FIRST",
                firstActionQuery, returningActionQuery, dimensionColumn,
                dimensionColumn, timeSubtraction, CONNECTOR_FIELD, CONNECTOR_FIELD,
                timeSubtraction, MAXIMUM_LEAD);

        return executor.executeRawQuery(query);
    }

    private String getPrecalculatedTable(String project, String collection, String schema, String timePredicate) {
        return String.format("select date as time, _user from %s where %s",
                executor.formatTableReference(project, QualifiedName.of(schema, "_users_daily_" + collection)), timePredicate);
    }

    private String generateQuery(String project,
                                 String collection,
                                 String connectorField,
                                 String timeColumn,
                                 Optional<String> dimension,
                                 Optional<Expression> filter,
                                 LocalDate startDate,
                                 LocalDate endDate) {

        if (!dimension.isPresent()) {
            if (continuousQueryService.list(project).stream().anyMatch(e -> e.tableName.equals("_users_daily_" + collection))) {
                return getPrecalculatedTable(project, collection, "continuous", String.format("date between date '%s' and date '%s' + interval '1' day",
                        startDate.format(ISO_LOCAL_DATE), endDate.format(ISO_LOCAL_DATE)));
            } else
            if (materializedViewService.list(project).stream().anyMatch(e -> e.tableName.equals("_users_daily_" + collection))) {
                return getPrecalculatedTable(project, collection, "materialized", String.format("date between date '%s' and date '%s' + interval '1' day",
                        startDate.format(ISO_LOCAL_DATE), endDate.format(ISO_LOCAL_DATE)));
            }
        } else {

        }

        return format("select %s, %s as time %s from %s where _time between date '%s' and date '%s' + interval '1' day %s group by 1, 2 %s",
                connectorField,
                timeColumn,
                dimension.isPresent() ? ", " + checkTableColumn(dimension.get(), "dimension") + " as dimension" : "",
                executor.formatTableReference(project, QualifiedName.of(collection)),
                startDate.format(ISO_LOCAL_DATE), endDate.format(ISO_LOCAL_DATE),
                filter.isPresent() ? "and " + formatExpression(filter.get(), reference -> executor.formatTableReference(project, reference)) : "",
                dimension.isPresent() ? ", 3" : "");
    }

}
