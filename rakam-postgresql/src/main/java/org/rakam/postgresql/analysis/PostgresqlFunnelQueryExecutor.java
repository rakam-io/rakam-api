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
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.analysis.FunnelQueryExecutor.FunnelStep;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.RakamExpressionFormatter.formatIdentifier;
import static com.facebook.presto.sql.RakamSqlFormatter.formatExpression;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.rakam.collection.FieldType.INTEGER;
import static org.rakam.collection.FieldType.STRING;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class PostgresqlFunnelQueryExecutor
        implements FunnelQueryExecutor
{
    private final PostgresqlQueryExecutor executor;
    private static final String CONNECTOR_FIELD = "_user";

    @Inject
    public PostgresqlFunnelQueryExecutor(PostgresqlQueryExecutor executor)
    {
        this.executor = executor;
    }

    @PostConstruct
    public void setup()
    {
        try (Connection conn = executor.getConnection()) {
            conn.createStatement().execute("CREATE EXTENSION IF NOT EXISTS plv8; \n" +
                    "CREATE OR REPLACE FUNCTION public.get_funnel_step(arr int[]) RETURNS integer AS $$\n" +
                    "DECLARE next_step integer := 1; step integer;\n" +
                    "        BEGIN \n" +
                    "                FOREACH step IN ARRAY arr\n" +
                    "       LOOP\n" +
                    "      IF step = next_step THEN\n" +
                    "         next_step = next_step + 1;\n" +
                    "      END IF;\n" +
                    "       END LOOP;\n" +
                    "    return next_step - 1;\n" +
                    "        END;\n" +
                    "$$ LANGUAGE plpgsql;");
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public QueryExecution query(String project, List<FunnelStep> steps, Optional<String> dimension, LocalDate startDate, LocalDate endDate, Optional<FunnelWindow> window)
    {
        if (dimension.isPresent() && CONNECTOR_FIELD.equals(dimension.get())) {
            throw new RakamException("Dimension and connector field cannot be equal", HttpResponseStatus.BAD_REQUEST);
        }
        String ctes = IntStream.range(0, steps.size())
                .mapToObj(i -> convertFunnel(project, CONNECTOR_FIELD, i, steps.get(i), dimension))
                .collect(Collectors.joining(" UNION ALL "));

        String dimensionCol = dimension.map(ValidationUtil::checkTableColumn).map(v -> v + ", ").orElse("");
        QueryExecution queryExecution = executor.executeRawQuery(format("select %s public.get_funnel_step(steps) step, count(*) from (\n" +
                        "select %s array_agg(step) as steps from (%s) t WHERE _time between date '%s' and date '%s'\n" +
                        "group by %s %s\n" +
                        ") t group by 1 %s order by 1", dimensionCol, dimensionCol, ctes,
                startDate.format(ISO_LOCAL_DATE),
                endDate.format(ISO_LOCAL_DATE),
                dimensionCol, CONNECTOR_FIELD,
                dimension.map(v -> ", 2").orElse("")));

        return new DelegateQueryExecution(queryExecution,
                result -> {
                    if (result.isFailed()) {
                        return result;
                    }

                    List<List<Object>> newResult;
                    List<List<Object>> queryResult = result.getResult();
                    List<SchemaField> metadata;
                    if (dimension.isPresent()) {
                        newResult = new ArrayList<>();
                        Map<Object, List<List<Object>>> collect = queryResult.stream().collect(Collectors.groupingBy(x -> x.get(0)));
                        for (Map.Entry<Object, List<List<Object>>> entry : collect.entrySet()) {
                            List<List<Object>> subResult = IntStream.range(0, steps.size())
                                    .mapToObj(i -> Arrays.asList("Step " + (i + 1), entry.getKey(), 0))
                                    .collect(Collectors.toList());

                            for (int step = 0; step < subResult.size(); step++) {
                                int finalStep = step;
                                entry.getValue().stream().filter(e -> ((Number) e.get(1)).intValue() >= finalStep + 1).map(e -> e.get(2))
                                        .forEach(val -> subResult.get(finalStep).set(2,
                                                ((Number) subResult.get(finalStep).get(2)).intValue() + ((Number) val).intValue()));
                            }

                            newResult.addAll(subResult);
                        }
                        metadata = ImmutableList.of(
                                new SchemaField("step", STRING),
                                new SchemaField("dimension", STRING),
                                new SchemaField("count", INTEGER));
                    }
                    else {
                        newResult = IntStream.range(0, steps.size())
                                .mapToObj(i -> Arrays.<Object>asList("Step " + (i + 1), 0))
                                .collect(Collectors.toList());

                        for (int step = 0; step < newResult.size(); step++) {
                            int finalStep = step;
                            queryResult.stream().filter(e -> ((Number) e.get(0)).intValue() >= finalStep + 1).map(e -> e.get(1))
                                    .forEach(val -> newResult.get(finalStep).set(1,
                                            ((Number) newResult.get(finalStep).get(1)).intValue() + ((Number) val).intValue()));
                        }

                        metadata = ImmutableList.of(
                                new SchemaField("step", STRING),
                                new SchemaField("count", INTEGER));
                    }
                    return new QueryResult(metadata, newResult, result.getProperties());
                });
    }

    private String convertFunnel(String project, String CONNECTOR_FIELD, int idx, FunnelStep funnelStep, Optional<String> dimension)
    {
        String table = project + "." + ValidationUtil.checkCollection(funnelStep.getCollection());
        Optional<String> filterExp = funnelStep.getExpression().map(value -> RakamSqlFormatter.formatExpression(value,
                name -> name.getParts().stream().map(RakamExpressionFormatter::formatIdentifier).collect(Collectors.joining(".")),
                name -> formatIdentifier("step" + idx) + "." + name.getParts().stream()
                        .map(RakamExpressionFormatter::formatIdentifier).collect(Collectors.joining("."))));

        return format("SELECT %s %s, %d as step, _time from %s %s %s",
                dimension.map(ValidationUtil::checkTableColumn).map(v -> v + ",").orElse(""), CONNECTOR_FIELD, idx + 1, table,
                "step" + idx,
                filterExp.map(v -> "where " + v).orElse(""));
    }
}

