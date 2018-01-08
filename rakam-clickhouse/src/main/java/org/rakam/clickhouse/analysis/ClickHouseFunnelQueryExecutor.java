package org.rakam.clickhouse.analysis;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.util.RakamException;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.RakamExpressionFormatter.formatIdentifier;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.stream.IntStream.range;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class ClickHouseFunnelQueryExecutor
        implements FunnelQueryExecutor {
    private final QueryExecutor queryExecutor;
    private final ProjectConfig projectConfig;

    @Inject
    public ClickHouseFunnelQueryExecutor(ProjectConfig projectConfig, QueryExecutor queryExecutor) {
        this.projectConfig = projectConfig;
        this.queryExecutor = queryExecutor;
    }

    private static int toSeconds(FunnelWindow window) {
        switch (window.type) {
            case DAY:
                return window.value * 86400;
            case WEEK:
                return window.value * 7 * 86400;
            case MONTH:
                return window.value * 30 * 86400;
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public QueryExecution query(String project, List<FunnelStep> steps,
                                Optional<String> dimension, Optional<String> segment, LocalDate startDate, LocalDate endDate,
                                Optional<FunnelWindow> window, ZoneId zoneId, Optional<List<String>> connectors, FunnelType funnelType) {
        if (steps.size() == 0) {
            throw new RakamException("Funnel steps parameter is empty", BAD_REQUEST);
        }

        String betweenWindow = window.map(ClickHouseFunnelQueryExecutor::toSeconds).map(i -> format("(?t<=%d)", i)).orElse("");

        String select = range(0, steps.size())
                .mapToObj(step -> step == 0 ? "count() as step1" : format("sum(step%d) as step%d", step + 1, step + 1))
                .collect(Collectors.joining(", "));

        String funnel = range(1, steps.size()).mapToObj(step -> format("sequenceMatch('%s')(_time, step = 1, %s) AS step%d",
                range(1, step + 2).mapToObj(i -> format("(?%d)", i)).collect(Collectors.joining(betweenWindow + ".*")),
                range(1, step + 1).mapToObj(i -> format("step = %d", i + 1)).collect(Collectors.joining(", ")),
                step + 1
        )).collect(Collectors.joining(", \n"));

        String computeQueries = IntStream.range(0, steps.size()).mapToObj(step -> {
            FunnelStep funnelStep = steps.get(step);
            String collection = funnelStep.getCollection();

            return format("SELECT %d as step, %s, %s %s FROM %s WHERE %s BETWEEN CAST(toDate('%s') AS DateTime) AND CAST(toDate('%s') AS DateTime) %s",
                    step + 1,
                    checkTableColumn(projectConfig.getUserColumn()),
                    checkTableColumn(projectConfig.getTimeColumn()),
                    dimension.map(v -> ", " + checkTableColumn(v, '`') + " as dimension").orElse(""),
                    project + "." + checkCollection(collection, '`'),
                    checkTableColumn(projectConfig.getTimeColumn()),
                    startDate.format(ISO_DATE),
                    endDate.plusDays(1).format(ISO_DATE),
                    funnelStep.getExpression().map(exp -> "AND " + ClickhouseExpressionFormatter.formatExpression(exp,
                            name -> name.getParts().stream().map(e -> formatIdentifier(e, '`')).collect(Collectors.joining(".")),
                            name -> checkCollection(funnelStep.getCollection()) + "." + name, '`')).orElse(""));
        }).collect(Collectors.joining("\n        UNION ALL\n     "));

        String query = format("SELECT\n" +
                        "    %s %s\n" +
                        " FROM\n" +
                        " (\n" +
                        "    SELECT\n" +
                        "        %s %s\n" +
                        "    FROM\n" +
                        "    (%s)\n" +
                        "    GROUP BY _user %s\n" +
                        "    HAVING any(step = 1) \n" +
                        " ) %s ",
                dimension.map(v -> "dimension, ").orElse(""),
                select,
                dimension.map(v -> "dimension, ").orElse(""),
                !dimension.isPresent() && funnel.isEmpty() ? "1" : funnel,
                computeQueries,
                dimension.map(v -> ", dimension").orElse(""),
                dimension.map(v -> "GROUP BY dimension WITH TOTALS ORDER BY " +
                        range(0, steps.size()).mapToObj(i ->
                                format("step%d DESC", i + 1)).collect(Collectors.joining(", ")) + " LIMIT 50")
                        .orElse(""));
        return new DelegateQueryExecution(queryExecutor.executeRawQuery(query, zoneId), result -> {
            List<List<Object>> data;
            if (!result.isFailed()) {
                if (dimension.isPresent()) {
                    data = new ArrayList<>(result.getResult().size() * steps.size());

                    long totalStep1 = 0;
                    int totalsIndex = result.getResult().size() - 1;
                    for (int i = 0; i < totalsIndex; i++) {
                        List<Object> objects = result.getResult().get(i);

                        Object dimensionValue = (objects.get(0) == null || objects.get(0).toString().length() == 0) ?
                                null : objects.get(0);

                        totalStep1 += (Long) objects.get(1);

                        for (int idx = 1; idx < objects.size(); idx++) {
                            data.add(Arrays.asList("Step " + idx, dimensionValue, objects.get(idx)));
                        }
                    }

                    List<Object> totalsRow = result.getResult().get(totalsIndex);
                    if (totalStep1 < ((Long) totalsRow.get(1))) {
                        for (int idx = 1; idx < totalsRow.size(); idx++) {
                            data.add(Arrays.asList("Step " + idx, "Others", totalsRow.get(idx)));
                        }
                    }

                    return new QueryResult(ImmutableList.of(
                            new SchemaField("step", FieldType.STRING),
                            new SchemaField("dimension", result.getMetadata().get(0).getType()),
                            new SchemaField("value", FieldType.LONG)), data);
                } else {
                    data = IntStream.range(0, steps.size())
                            .mapToObj(step -> ImmutableList.of("Step " + (step + 1),
                                    result.getResult().isEmpty() ? 0L : result.getResult().get(0).get(step)))
                            .collect(Collectors.toList());

                    return new QueryResult(ImmutableList.of(
                            new SchemaField("step", FieldType.STRING),
                            new SchemaField("value", FieldType.LONG)), data);
                }
            }

            return result;
        });
    }
}
