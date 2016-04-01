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
package org.rakam.postgresql.analysis;

import com.facebook.presto.sql.RakamExpressionFormatter;
import com.facebook.presto.sql.RakamSqlFormatter;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.QueryExecution;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.RakamExpressionFormatter.formatIdentifier;

public class PostgresqlFunnelQueryExecutor implements FunnelQueryExecutor {
    private final PostgresqlQueryExecutor executor;
    private static final String CONNECTOR_FIELD = "_user";

    @Inject
    public PostgresqlFunnelQueryExecutor(PostgresqlQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public QueryExecution query(String project, List<FunnelQueryExecutor.FunnelStep> steps, Optional<String> dimension, LocalDate startDate, LocalDate endDate, int windowDays, WindowType windowType) {
        if (dimension.isPresent() && CONNECTOR_FIELD.equals(dimension.get())) {
            throw new RakamException("Dimension and connector field cannot be equal", HttpResponseStatus.BAD_REQUEST);
        }
        String ctes = IntStream.range(0, steps.size())
                .mapToObj(i -> convertFunnel(project, CONNECTOR_FIELD, i, steps.get(i), dimension, startDate, endDate))
                .collect(Collectors.joining(", "));

        String query;
        if (dimension.isPresent()) {
            query = IntStream.range(0, steps.size())
                    .mapToObj(i -> String.format("(SELECT step, CASE WHEN rank > 15 THEN 'Others' ELSE %s END, sum(count) FROM (select CAST('Step %d' as varchar) as step, %s, count(*) count, row_number() OVER(ORDER BY 3 DESC) rank from step%s GROUP BY 2 ORDER BY 4 ASC) data GROUP BY 1, 2 ORDER BY 3 DESC)",
                            dimension.get(), i + 1, dimension.get(), i))
                    .collect(Collectors.joining(" UNION ALL "));
        } else {
            query = IntStream.range(0, steps.size())
                    .mapToObj(i -> String.format("(SELECT cast('Step %d' as varchar) as step, count(*) count FROM step%s)",
                            i + 1, i))
                    .collect(Collectors.joining(" UNION ALL "));
        }
        return executor.executeRawQuery("WITH \n" + ctes + " " + query);
    }

    private String convertFunnel(String project, String CONNECTOR_FIELD, int idx, FunnelQueryExecutor.FunnelStep funnelStep, Optional<String> dimension, LocalDate startDate, LocalDate endDate) {
        String table = project + "." + funnelStep.getCollection();
        ZoneId utc = ZoneId.of("UTC");
        long startTs = startDate.atStartOfDay().atZone(utc).toEpochSecond();
        long endTs = endDate.atStartOfDay().atZone(utc).toEpochSecond();
        String filterExp = funnelStep.getExpression().map(value -> "AND " + RakamSqlFormatter.formatExpression(value,
                name -> name.getParts().stream().map(RakamExpressionFormatter::formatIdentifier).collect(Collectors.joining(".")),
                name -> formatIdentifier(funnelStep.getCollection()) + "." + name.getParts().stream()
                        .map(RakamExpressionFormatter::formatIdentifier).collect(Collectors.joining(".")))).orElse("");

        String dimensionColumn = dimension.isPresent() ? dimension.get() + "," : "";

        if (idx == 0) {
            return String.format("step0 AS (select %s %s from %s step0 where _time BETWEEN to_timestamp(%s) and to_timestamp(%s) %s\n group by 1 %s)",
                    dimensionColumn, CONNECTOR_FIELD, table, startTs, endTs,
                    filterExp, dimension.isPresent() ? ", 2" : "");
        } else {
            return String.format("%1$s AS (\n" +
                            "select %7$s %1$s.%9$s from %2$s %1$s join %3$s on (%1$s.%9$s = %3$s.%9$s) " +
                            "where _time BETWEEN to_timestamp(%5$s) and to_timestamp(%6$s) %4$s group by 1 %8$s)",
                    "step" + idx, table, "step" + (idx - 1), filterExp, startTs,
                    endTs, dimensionColumn.isEmpty() ? "" : "step" + idx + "." + dimensionColumn,
                    dimension.isPresent() ? ", 2" : "",
                    CONNECTOR_FIELD);
        }
    }
}

