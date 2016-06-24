package org.rakam.clickhouse.analysis;

import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.report.QueryExecution;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

public class ClickHouseFunnelQueryExecutor
        implements FunnelQueryExecutor
{

    @Override
    public QueryExecution query(String project, List<FunnelStep> steps, Optional<String> dimension, LocalDate startDate, LocalDate endDate, Optional<FunnelWindow> window)
    {
        return null;
    }
}
