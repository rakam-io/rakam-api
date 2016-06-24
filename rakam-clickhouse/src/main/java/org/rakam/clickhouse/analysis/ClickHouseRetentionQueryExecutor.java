package org.rakam.clickhouse.analysis;

import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.report.QueryExecution;

import java.time.LocalDate;
import java.util.Optional;

/**
 * Created by buremba on 6/20/16.
 */
public class ClickHouseRetentionQueryExecutor
        implements RetentionQueryExecutor
{
    @Override
    public QueryExecution query(String project, Optional<RetentionAction> firstAction, Optional<RetentionAction> returningAction, DateUnit dateUnit, Optional<String> dimension, Optional<Integer> period, LocalDate startDate, LocalDate endDate)
    {

//        var vb = [];
//        for(var i = 0; i< 30; i++) {
//            vb.push("select toDate('"+func(new Date(1987, 10, i))+"'), FlightDate, sum(FlightNum IN (select FlightNum from ontime WHERE FlightDate = toDate('"+func(new Date(1987, 10, i))+"'))) as ratio from ontime where FlightDate between toDate('"+func(new Date(1987, 10, i))+"') and toDate('"+func(new Date(1987, 10, i+15))+"') group by FlightDate order by FlightDate");
//        }
//        vb.join(" union all ")
        return null;
    }
}
