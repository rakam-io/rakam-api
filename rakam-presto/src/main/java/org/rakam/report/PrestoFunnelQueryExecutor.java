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
package org.rakam.report;

import com.facebook.presto.sql.ExpressionFormatter;
import com.google.inject.Inject;
import org.rakam.analysis.FunnelQueryExecutor;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/08/15 15:27.
 */
public class PrestoFunnelQueryExecutor implements FunnelQueryExecutor {
    private final PrestoQueryExecutor executor;
    private final String coldStorageConnector;

    @Inject
    public PrestoFunnelQueryExecutor(PrestoQueryExecutor executor, PrestoConfig config) {
        this.executor = executor;
        coldStorageConnector = config.getColdStorageConnector();
    }

    @Override
    public QueryExecution query(String project, List<FunnelStep> steps, Optional<String> dimension, LocalDate startDate, LocalDate endDate, boolean groupOthers) {
        String ctes = IntStream.range(0, steps.size())
                .mapToObj(i -> convertFunnel(project, i, steps.get(i), dimension, startDate, endDate))
                .collect(Collectors.joining(", "));

        String query;
        if(dimension.isPresent()) {
            if(groupOthers) {
                query =  IntStream.range(0, steps.size())
                        .mapToObj(i -> String.format("SELECT step, CASE WHEN rank > 15 THEN 'Others' ELSE %s END, sum(count) FROM (select 'Step %d' as step, %s, count(*) count, row_number() OVER(ORDER BY 3 DESC) rank from step%s GROUP BY 2 ORDER BY 4 ASC) GROUP BY 1, 2 ORDER BY 3 DESC",
                                dimension.get(), i+1, dimension.get(), i))
                        .collect(Collectors.joining(" union all "));
            } else {
                query = IntStream.range(0, steps.size())
                        .mapToObj(i -> String.format("select 'Step %d' as step, %s, count(*) count from step%d GROUP BY 2 ORDER BY 3 DESC",
                                i + 1, dimension.get(), i))
                        .collect(Collectors.joining(" union all "));
            }
        } else {
            query = IntStream.range(0, steps.size())
                    .mapToObj(i -> String.format("select 'Step %d' as step, count(*) count from step%s",
                            i+1, i))
                    .collect(Collectors.joining(" union all "));
        }
        return executor.executeRawQuery("WITH \n"+ ctes + " " + query);
    }

    private String convertFunnel(String project, int idx, FunnelStep funnelStep, Optional<String> dimension, LocalDate startDate, LocalDate endDate) {
        String table = coldStorageConnector + "." + project + "." + funnelStep.collection();
        ZoneId utc = ZoneId.of("UTC");
        long startTs = startDate.atStartOfDay().atZone(utc).toEpochSecond();
        long endTs = endDate.atStartOfDay().atZone(utc).toEpochSecond();
        String filterExp = funnelStep.filterExpression()
                .map(exp -> ", " + exp.accept(new ExpressionFormatter.Formatter(), false))
                .orElseGet(() -> "");

        String dimensionColumn = dimension.isPresent() ? dimension.get()+"," : "";

        if(idx == 0) {
            return String.format("step%s AS (select %s \"user\" from %s where time BETWEEN %s and %s %s\n group by 1 %s)",
                    idx, dimensionColumn, table, startTs, endTs,
                    filterExp, dimension.isPresent() ? ", 2" : "");
        } else {
            return String.format("%1$s AS (\n" +
                    "select %7$s %1$s.\"user\" from %2$s %1$s join %3$s on (%1$s.\"user\" = %3$s.\"user\") " +
                    "where time BETWEEN %5$s and %6$s %4$s group by 1 %8$s)",
                    "step"+idx, table, "step"+(idx-1), filterExp, startTs,
                    endTs, dimensionColumn, dimension.isPresent() ? ", 2" : "");
        }
    }
}
