package org.rakam.clickhouse.analysis;

import com.google.inject.Inject;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClickHouseRetentionQueryExecutor
        implements RetentionQueryExecutor
{
    private final QueryExecutor queryExecutor;

    @Inject
    public ClickHouseRetentionQueryExecutor(QueryExecutor queryExecutor)
    {
        this.queryExecutor = queryExecutor;
    }

    @Override
    public QueryExecution query(String project, Optional<RetentionAction> firstAction, Optional<RetentionAction> returningAction, DateUnit dateUnit, Optional<String> dimension, Optional<Integer> period, LocalDate startDate, LocalDate endDate)
    {

        int startEpoch = (int) startDate.toEpochDay();
        int endEpoch = (int) endDate.toEpochDay();

        IntStream.range(startEpoch, endEpoch).mapToObj(epoch -> {
            String date = LocalDate.ofEpochDay(epoch).format(DateTimeFormatter.ISO_DATE);
            String a = String.format("select toDate('%s'), _time, sum(_user IN (select _user from ontime WHERE $date = toDate('%s'))) as ratio from" +
                    " ontime where $date between toDate('%s') and toDate('%ne kadar gideceği') " +
                    " group by $date order by $date");
            return a;
        }).collect(Collectors.joining(" UNION ALL"));
        return null;
    }
}
