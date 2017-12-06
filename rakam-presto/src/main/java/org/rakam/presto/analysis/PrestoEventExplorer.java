/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.presto.analysis;

import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.config.ProjectConfig;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.eventexplorer.AbstractEventExplorer;
import org.rakam.report.realtime.AggregationType;

import javax.inject.Inject;
import java.util.Map;

import static org.rakam.analysis.EventExplorer.TimestampTransformation.*;

public class PrestoEventExplorer extends AbstractEventExplorer {
    private static final Map<TimestampTransformation, String> timestampMapping = ImmutableMap.
            <TimestampTransformation, String>builder()
            .put(HOUR_OF_DAY, "lpad(cast(hour(%s) as varchar), 2, '0')||':00'")
            .put(DAY_OF_MONTH, "cast(day(%s) as varchar)||'th day'")
            .put(WEEK_OF_YEAR, "cast(week(%s) as varchar)||'th week'")
            .put(MONTH_OF_YEAR, "date_format(%s, '%%M')")
            .put(QUARTER_OF_YEAR, "cast(quarter(%s) as varchar)||'th quarter'")
            .put(DAY_OF_WEEK, "date_format(%s, '%%W')")
            .put(HOUR, "date_trunc('hour', %s)")
            .put(DAY, "cast(%s as date)")
            .put(WEEK, "cast(date_trunc('week', %s) as date)")
            .put(MONTH, "cast(date_trunc('month', %s) as date)")
            .put(YEAR, "cast(date_trunc('year', %s) as date)")
            .build();

    @Inject
    public PrestoEventExplorer(ProjectConfig projectConfig, QueryExecutorService executor,
                               MaterializedViewService materializedViewService) {
        super(projectConfig, executor, materializedViewService, timestampMapping);
    }

    @Override
    public String convertSqlFunction(AggregationType aggType) {
        switch (aggType) {
            case AVERAGE:
                return "avg(%s)";
            case MAXIMUM:
                return "max(%s)";
            case MINIMUM:
                return "min(%s)";
            case COUNT:
                return "count(%s)";
            case SUM:
                return "sum(%s)";
            case COUNT_UNIQUE:
                return "count(distinct %s)";
            case APPROXIMATE_UNIQUE:
                return "approx_distinct(%s)";
            default:
                throw new IllegalArgumentException("aggregation type is not supported");
        }
    }

    @Override
    public String getIntermediateForApproximateUniqueFunction() {
        return "CAST(approx_set(%s) AS VARBINARY)";
    }

    @Override
    public String getFinalForApproximateUniqueFunction() {
        return "cardinality(CAST(%s AS HYPERLOGLOG))";
    }
}