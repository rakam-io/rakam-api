package org.rakam.clickhouse.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.google.inject.Inject;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.config.ProjectConfig;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.util.ValidationUtil;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.sql.RakamExpressionFormatter.formatIdentifier;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class ClickHouseRetentionQueryExecutor
        implements RetentionQueryExecutor {
    private final QueryExecutor executor;
    private final Metastore metastore;
    private final ProjectConfig projectConfig;

    @Inject
    public ClickHouseRetentionQueryExecutor(ProjectConfig projectConfig, QueryExecutor executor, Metastore metastore) {
        this.projectConfig = projectConfig;
        this.executor = executor;
        this.metastore = metastore;
    }

    private static String formatExpression(Expression value) {
        return RakamSqlFormatter.formatExpression(value,
                name -> name.getParts().stream().map(e -> formatIdentifier(e, '`')).collect(Collectors.joining(".")),
                ValidationUtil::checkTableColumn, '`');
    }

    @Override
    public QueryExecution query(String project,
                                Optional<RetentionAction> firstAction,
                                Optional<RetentionAction> returningAction,
                                DateUnit dateUnit,
                                Optional<String> dimension,
                                Optional<Integer> period,
                                LocalDate startDate,
                                LocalDate endDate, ZoneId zoneId,
                                boolean approximate) {
        int startEpoch = (int) startDate.toEpochDay();
        int endEpoch = (int) endDate.toEpochDay();

        String firstActionQuery = firstAction.map(action -> format("SELECT `$date`, %s, %s %s FROM %s.%s %s",
                checkTableColumn(projectConfig.getUserColumn(), '`'), checkTableColumn(projectConfig.getTimeColumn(), '`'),
                dimension.map(e -> "," + e).orElse(""), project, ValidationUtil.checkCollection(action.collection(), '`'),
                action.filter().map(f -> "WHERE " + formatExpression(f)).orElse("")))
                .orElseGet(() -> metastore.getCollectionNames(project).stream()
                        .map(collection -> format("SELECT `$date`, %s, %s %s FROM %s.%s",
                                checkTableColumn(projectConfig.getUserColumn(), '`'), checkTableColumn(projectConfig.getTimeColumn(), '`'),
                                dimension.map(e -> "," + e).orElse(""), project, ValidationUtil.checkCollection(collection, '`')))
                        .collect(Collectors.joining(" UNION ALL ")));

        String query = IntStream.range(startEpoch, endEpoch).boxed().flatMap(epoch -> {
            LocalDate beginDate = LocalDate.ofEpochDay(epoch);
            String date = startDate.format(ISO_DATE);
            String endDateStr = period.map(e -> beginDate.plus(e, dateUnit.getTemporalUnit()))
                    .map(e -> e.isAfter(endDate) ? endDate : e)
                    .orElse(LocalDate.ofEpochDay(endEpoch)).format(ISO_DATE);

            return Stream.of(format("select %s, CAST(-1 AS Int64) as lead, uniq(_user) users from" +
                            " (%s) where `$date` between toDate('%s')  and toDate('%s') " +
                            " group by `$date` %s",
                    dimension.map(e -> checkTableColumn(e, '`')).orElse(format("toDate('%s') as date", date)),
                    firstActionQuery,
                    date, endDateStr, dimension.map(e -> "," + checkCollection(e, '`')).orElse("")),
                    format("select %s, (`$date` - toDate('%s')) - 1 as lead, sum(_user IN (select _user from (%s) WHERE `$date` = toDate('%s'))) as users from" +
                                    " (%s) where `$date` between toDate('%s') and toDate('%s') " +
                                    " group by `$date` %s order by %s",
                            dimension.map(e -> checkTableColumn(e, '`')).orElse(format("toDate('%s') as date", date)),
                            date,
                            firstActionQuery,
                            date,
                            firstActionQuery,
                            date, endDateStr,
                            dimension.map(e -> ", " + checkCollection(e, '`')).orElse(""),
                            dimension.map(e -> checkCollection(e, '`')).orElse("`$date`")));
        }).collect(Collectors.joining(" UNION ALL \n"));

        return new DelegateQueryExecution(executor.executeRawQuery(query, zoneId), (result) -> {
            if (result.isFailed()) {
                return result;
            }

            Long uniqUser = new Long(-1);

            result.getResult().stream()
                    .filter(objects -> objects.get(1).equals(uniqUser))
                    .forEach(objects -> objects.set(1, null));

            return result;
        });
    }
}
