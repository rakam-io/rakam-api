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

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.RakamSqlFormatter.ExpressionFormatter;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.RakamSqlFormatter.ExpressionFormatter.formatIdentifier;

public class PrestoFunnelQueryExecutor implements FunnelQueryExecutor {
    private final QueryExecutorService executor;
    private static final String CONNECTOR_FIELD = "_user";

    @Inject
    public PrestoFunnelQueryExecutor(QueryExecutorService executor) {
        this.executor = executor;
    }

    @Override
    public QueryExecution query(String project, List<FunnelQueryExecutor.FunnelStep> steps, Optional<String> dimension, LocalDate startDate, LocalDate endDate, boolean groupOthers) {
        if(dimension.isPresent() && CONNECTOR_FIELD.equals(dimension.get())) {
            throw new RakamException("Dimension and connector field cannot be equal", HttpResponseStatus.BAD_REQUEST);
        }
        if(groupOthers && !dimension.isPresent()) {
            throw new RakamException("Dimension is required when grouping.", HttpResponseStatus.BAD_REQUEST);
        }

        String ctes = IntStream.range(0, steps.size())
                .mapToObj(i -> convertFunnel(CONNECTOR_FIELD, i, steps.get(i), dimension, startDate, endDate))
                .collect(Collectors.joining(", "));

        String query;
        if(dimension.isPresent()) {
            if(groupOthers) {
                query =  IntStream.range(0, steps.size())
                        .mapToObj(i -> String.format("(SELECT step, CASE WHEN rank > 15 THEN 'Others' ELSE cast(%s as varchar) END, sum(count) FROM (select 'Step %d' as step, %s, count(*) count, row_number() OVER(ORDER BY 3 DESC) rank from step%s GROUP BY 2 ORDER BY 4 ASC) GROUP BY 1, 2 ORDER BY 3 DESC)",
                                dimension.get(), i+1, dimension.get(), i))
                        .collect(Collectors.joining(" UNION ALL "));
            } else {
                query = IntStream.range(0, steps.size())
                        .mapToObj(i -> String.format("(SELECT 'Step %d' as step, %s, count(*) count from step%d GROUP BY 2 ORDER BY 3 DESC)",
                                i + 1, dimension.get(), i))
                        .collect(Collectors.joining(" UNION ALL "));
            }
        } else {
            query = IntStream.range(0, steps.size())
                    .mapToObj(i -> String.format("(SELECT 'Step %d' as step, count(*) count FROM step%s)",
                            i+1, i))
                    .collect(Collectors.joining(" UNION ALL "));
        }
        return executor.executeQuery(project, "WITH \n" + ctes + " " + query + " ORDER BY 1 ASC");
    }

    private String convertFunnel(String CONNECTOR_FIELD, int idx, FunnelQueryExecutor.FunnelStep funnelStep, Optional<String> dimension, LocalDate startDate, LocalDate endDate) {
        ZoneId utc = ZoneId.of("UTC");
        long startTs = startDate.atStartOfDay().atZone(utc).toEpochSecond();
        long endTs = endDate.atStartOfDay().atZone(utc).toEpochSecond();
        String filterExp = funnelStep.filterExpression != null && !funnelStep.filterExpression.isEmpty() ?
                "AND " + RakamSqlFormatter.formatExpression(funnelStep.getExpression(),
                        name -> name.getParts().stream() .map(ExpressionFormatter::formatIdentifier).collect(Collectors.joining(".")),
                        name -> formatIdentifier(funnelStep.collection)+"."+name.getParts().stream()
                                .map(ExpressionFormatter::formatIdentifier).collect(Collectors.joining("."))) : "";

        String dimensionColumn = dimension.isPresent() ? dimension.get()+"," : "";

        if(idx == 0) {
            return String.format("step%s AS (select %s %s from %s where _time BETWEEN from_unixtime(%s) and from_unixtime(%s) + interval '1' day %s\n group by 1 %s)",
                    idx, dimensionColumn, CONNECTOR_FIELD, funnelStep.collection, startTs, endTs,
                    filterExp, dimension.isPresent() ? ", 2" : "");
        } else {
            return String.format("%1$s AS (\n" +
                            "select %7$s %2$s.%9$s from %2$s join %3$s on (%2$s.%9$s = %3$s.%9$s) " +
                            "where _time BETWEEN from_unixtime(%5$s) and from_unixtime(%6$s) + interval '1' day %4$s group by 1 %8$s)",
                    "step"+idx, funnelStep.collection, "step"+(idx-1), filterExp, startTs,
                    endTs, dimensionColumn.isEmpty() ? "" : funnelStep.collection+"."+dimensionColumn,
                    dimension.isPresent() ? ", 2" : "",
                    CONNECTOR_FIELD);
        }
    }
}
