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
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.report.realtime.AggregationType;
import org.rakam.report.eventexplorer.AbstractEventExplorer;
import org.rakam.report.QueryExecutorService;

import javax.inject.Inject;
import java.util.Map;

import static org.rakam.analysis.EventExplorer.TimestampTransformation.*;

public class PostgresqlEventExplorer extends AbstractEventExplorer {
    private static final Map<TimestampTransformation, String> timestampMapping = ImmutableMap.
            <TimestampTransformation, String>builder()
            .put(HOUR_OF_DAY, "cast(extract(hour from %s) as bigint)")
            .put(DAY_OF_MONTH, "cast(extract(day FROM %s) as bigint)")
            .put(WEEK_OF_YEAR, "cast(extract(doy FROM %s) as bigint)")
            .put(MONTH_OF_YEAR, "cast(extract(month FROM %s) as bigint)")
            .put(QUARTER_OF_YEAR, "cast(extract(quarter FROM %s) as bigint)")
            .put(DAY_OF_WEEK, "cast(extract(dow FROM %s) as bigint)")
            .put(HOUR, "date_trunc('hour', %s)")
            .put(DAY, "cast(%s as date)")
            .put(MONTH, "date_trunc('month', %s)")
            .put(YEAR, "date_trunc('year', %s)")
            .build();

    @Inject
    public PostgresqlEventExplorer(QueryExecutorService service, MaterializedViewService materializedViewService,
                                   ContinuousQueryService continuousQueryService, PostgresqlQueryExecutor executor, Metastore metastore) {
        super(executor, materializedViewService, continuousQueryService, service, metastore, timestampMapping);
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
}