package org.rakam.clickhouse.analysis;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.collection.SchemaField;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.time.format.DateTimeFormatter.ISO_DATE;
import static org.rakam.collection.FieldType.INTEGER;

public class ClickHouseRetentionQueryExecutor
        implements RetentionQueryExecutor
{
    private final QueryExecutor executor;

    @Inject
    public ClickHouseRetentionQueryExecutor(QueryExecutor executor)
    {
        this.executor = executor;
    }

    @Override
    public QueryExecution query(String project, Optional<RetentionAction> firstAction, Optional<RetentionAction> returningAction, DateUnit dateUnit, Optional<String> dimension, Optional<Integer> period, LocalDate startDate, LocalDate endDate)
    {
        int startEpoch = (int) startDate.toEpochDay();
        int endEpoch = (int) endDate.toEpochDay();

        String query = IntStream.range(startEpoch, endEpoch).boxed().flatMap(epoch -> {
            LocalDate beginDate = LocalDate.ofEpochDay(epoch);
            String date = startDate.format(ISO_DATE);
            String endDateStr = period.map(e -> beginDate.plus(e, dateUnit.getTemporalUnit()))
                    .map(e -> e.isAfter(endDate) ? endDate : e)
                    .orElse(LocalDate.ofEpochDay(endEpoch)).format(ISO_DATE);

            return Stream.of(String.format("select toDate('%s') as date, CAST(-1 AS Int64) as lead, uniq(_user) users from" +
                            " %s where `$date` between toDate('%s')  and toDate('%s') " +
                            " group by `$date`", date,
                    project + "." + firstAction.get().collection(),
                    date, endDateStr),

                    String.format("select toDate('%s') as date, (`$date` - toDate('%s')) - 1 as lead, sum(_user IN (select _user from %s WHERE `$date` = toDate('%s'))) as users from" +
                            " %s where `$date` between toDate('%s') and toDate('%s') " +
                            " group by `$date` order by `$date`", date, date,
                    project + "." + firstAction.get().collection(),
                    date,
                    project + "." + returningAction.get().collection(),
                    date, endDateStr));
        }).collect(Collectors.joining(" UNION ALL \n"));

        return new DelegateQueryExecution(executor.executeRawQuery(query), (result) -> {
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
