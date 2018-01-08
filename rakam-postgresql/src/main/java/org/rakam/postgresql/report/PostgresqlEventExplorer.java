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
package org.rakam.postgresql.report;

import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.config.ProjectConfig;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.eventexplorer.AbstractEventExplorer;
import org.rakam.report.realtime.AggregationType;

import javax.inject.Inject;
import java.util.Map;

import static java.lang.String.format;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.*;

public class PostgresqlEventExplorer
        extends AbstractEventExplorer {
    private static final Map<TimestampTransformation, String> timestampMapping = ImmutableMap.
            <TimestampTransformation, String>builder()
            .put(HOUR_OF_DAY, "lpad(cast(extract(hour FROM %s) as text), 2, '0')||':00'")
            .put(DAY_OF_MONTH, "extract(day FROM %s)||'th day'")
            .put(WEEK_OF_YEAR, "extract(doy FROM %s)||'th week'")
            .put(MONTH_OF_YEAR, "rtrim(to_char(%s, 'Month'))")
            .put(QUARTER_OF_YEAR, "extract(quarter FROM %s)||'th quarter'")
            .put(DAY_OF_WEEK, "rtrim(to_char(%s, 'Day'))")
            .put(HOUR, "date_trunc('hour', %s)")
            .put(DAY, "cast(%s as date)")
            .put(MONTH, "cast(date_trunc('month', %s) as date)")
            .put(YEAR, "cast(date_trunc('year', %s) as date)")
            .build();

    @Inject
    public PostgresqlEventExplorer(ProjectConfig projectConfig, QueryExecutorService service, MaterializedViewService materializedViewService) {
        super(projectConfig, service, materializedViewService, timestampMapping);
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
                return "count(distinct %s)";
            default:
                throw new IllegalArgumentException("aggregation type is not supported");
        }
    }

    @Override
    public String convertSqlFunction(AggregationType intermediate, AggregationType main) {
        String column = convertSqlFunction(intermediate);
        if (intermediate == AggregationType.SUM && main == AggregationType.COUNT) {
            return format("cast(%s as bigint)", column);
        }

        return column;
    }
}