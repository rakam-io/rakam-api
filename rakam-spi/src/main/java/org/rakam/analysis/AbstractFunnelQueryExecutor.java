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
package org.rakam.analysis;

import com.facebook.presto.sql.RakamExpressionFormatter;
import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.SchemaField;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.RakamExpressionFormatter.formatIdentifier;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.rakam.collection.FieldType.LONG;
import static org.rakam.collection.FieldType.STRING;
import static org.rakam.util.ValidationUtil.checkCollection;

public abstract class AbstractFunnelQueryExecutor
{
    private static final String CONNECTOR_FIELD = "_user";
    private final QueryExecutor executor;

    public AbstractFunnelQueryExecutor(QueryExecutor executor)
    {
        this.executor = executor;
    }

    public QueryExecution query(String project,
            List<FunnelStep> steps,
            Optional<String> dimension, LocalDate startDate,
            LocalDate endDate, Optional<FunnelWindow> window)
    {
        if (dimension.isPresent() && CONNECTOR_FIELD.equals(dimension.get())) {
            throw new RakamException("Dimension and connector field cannot be equal", HttpResponseStatus.BAD_REQUEST);
        }
        String ctes = IntStream.range(0, steps.size())
                .mapToObj(i -> convertFunnel(project, CONNECTOR_FIELD, i, steps.get(i), dimension))
                .collect(Collectors.joining(" UNION ALL "));

        String dimensionCol = dimension.map(ValidationUtil::checkTableColumn).map(v -> v + ", ").orElse("");
        String query = format("select %s get_funnel_step(steps) step, count(*) total from (\n" +
                        "select %s array_agg(step) as steps from (%s) t WHERE _time between date '%s' and date '%s'\n" +
                        "group by %s %s\n" +
                        ") t group by 1 %s order by 1", dimensionCol, dimensionCol, ctes,
                startDate.format(ISO_LOCAL_DATE),
                endDate.format(ISO_LOCAL_DATE),
                dimensionCol, CONNECTOR_FIELD,
                dimension.map(v -> ", 2").orElse(""));
        if(dimension.isPresent()) {
            query = String.format("SELECT (CASE WHEN rank > 15 THEN 'Others' ELSE cast(%s as varchar) END) as dimension, step, sum(total) from " +
                            "(select *, row_number() OVER(ORDER BY total DESC) rank from (%s) t) t GROUP BY 1, 2",
                    dimension.map(ValidationUtil::checkTableColumn).get(), query);
        }
        QueryExecution queryExecution = executor.executeRawQuery(query);

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
                        Map<Object, List<List<Object>>> collect = queryResult.stream().collect(Collectors.groupingBy(x -> x.get(0) == null ? "null" : x.get(0)));
                        for (Map.Entry<Object, List<List<Object>>> entry : collect.entrySet()) {
                            List<List<Object>> subResult = IntStream.range(0, steps.size())
                                    .mapToObj(i -> Arrays.asList("Step " + (i + 1), entry.getKey(), 0L))
                                    .collect(Collectors.toList());

                            for (int step = 0; step < subResult.size(); step++) {
                                int finalStep = step;
                                entry.getValue().stream().filter(e -> ((Number) e.get(1)).longValue() >= finalStep + 1).map(e -> e.get(2))
                                        .forEach(val -> subResult.get(finalStep).set(2,
                                                ((Number) subResult.get(finalStep).get(2)).longValue() + ((Number) val).longValue()));
                            }

                            newResult.addAll(subResult);
                        }

                        metadata = ImmutableList.of(
                                new SchemaField("step", STRING),
                                new SchemaField("dimension", STRING),
                                new SchemaField("count", LONG));
                    }
                    else {
                        newResult = IntStream.range(0, steps.size())
                                .mapToObj(i -> Arrays.<Object>asList("Step " + (i + 1), 0L))
                                .collect(Collectors.toList());

                        for (int step = 0; step < newResult.size(); step++) {
                            int finalStep = step;
                            queryResult.stream().filter(e -> ((Number) e.get(0)).intValue() >= finalStep + 1).map(e -> e.get(1))
                                    .forEach(val -> newResult.get(finalStep).set(1,
                                            ((Number) newResult.get(finalStep).get(1)).longValue() + ((Number) val).longValue()));
                        }

                        metadata = ImmutableList.of(
                                new SchemaField("step", STRING),
                                new SchemaField("count", LONG));
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

    private static SqlParser parser = new SqlParser();

    public static class FunnelStep {
        private final String collection;
        private final Optional<String> filterExpression;

        @JsonCreator
        public FunnelStep(@JsonProperty("collection") String collection,
                          @JsonProperty("filterExpression") Optional<String> filterExpression) {
            checkCollection(collection);
            this.collection = collection;
            this.filterExpression = filterExpression == null ? Optional.<String>empty() : filterExpression;
        }

        public String getCollection() {
            return collection;
        }

        @JsonIgnore
        public synchronized Optional<Expression> getExpression() {
            return filterExpression.map(value -> parser.createExpression(value));
        }
    }

    public static enum WindowType {
        DAY, WEEK, MONTH;

        @JsonCreator
        public static WindowType get(String name) {
            return valueOf(name.toUpperCase());
        }

        @JsonProperty
        public String value() {
            return name();
        }
    }

    public static class FunnelWindow {
        public final int value;
        public final WindowType type;

        @JsonCreator
        public FunnelWindow(@JsonProperty("value") int value,
                            @JsonProperty("type") WindowType type) {
            this.value = value;
            this.type = type;
        }
    }
}
