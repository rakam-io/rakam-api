package org.rakam.postgresql.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import javax.inject.Inject;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.RakamExpressionFormatter.formatIdentifier;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static org.rakam.collection.FieldType.LONG;
import static org.rakam.collection.FieldType.STRING;
import static org.rakam.util.DateTimeUtils.TIMESTAMP_FORMATTER;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class FastGenericFunnelQueryExecutor
        implements FunnelQueryExecutor
{
    private final ProjectConfig projectConfig;
    private final QueryExecutorService executor;

    @Inject
    public FastGenericFunnelQueryExecutor(QueryExecutorService executor, ProjectConfig projectConfig)
    {
        this.projectConfig = projectConfig;
        this.executor = executor;
    }

    @Override
    public QueryExecution query(String project, List<FunnelStep> steps, Optional<String> dimension, LocalDate startDate, LocalDate endDate, Optional<FunnelWindow> window, ZoneId timezone, Optional<List<String>> connectors, Optional<Boolean> ordered)
    {
        if (ordered.isPresent() && ordered.get()) {
            throw new RakamException("Strict ordered funnel query is not supported", BAD_REQUEST);
        }

        if (dimension.isPresent() && connectors.isPresent() && connectors.get().contains(dimension)) {
            throw new RakamException("Dimension and connector field cannot be equal", BAD_REQUEST);
        }

        List<String> selects = new ArrayList<>();
        List<String> insideSelect = new ArrayList<>();
        List<String> mainSelect = new ArrayList<>();

        String connectorString = connectors.map(item -> item.stream().map(ValidationUtil::checkTableColumn).collect(Collectors.joining(", ")))
                .orElse(checkTableColumn(projectConfig.getUserColumn()));

        for (int i = 0; i < steps.size(); i++) {
            Optional<String> filterExp = steps.get(i).getExpression().map(value -> RakamSqlFormatter.formatExpression(value,
                    name -> name.getParts().stream().map(e -> formatIdentifier(e, '"')).collect(Collectors.joining(".")),
                    name -> name.getParts().stream()
                            .map(e -> formatIdentifier(e, '"')).collect(Collectors.joining(".")), '"'));

            if (i == 0) {
                selects.add(format("sum(case when ts_event%d is not null then 1 else 0 end) as event%d_count", i, i));
            }
            else {
                selects.add(format("sum(case when ts_event%d >= ts_event%d then 1 else 0 end) as event%d_count", i, i - 1, i));
            }

            insideSelect.add(format("min(case when step = %d then %s end) as ts_event%d", i, checkTableColumn(projectConfig.getTimeColumn()), i));
            mainSelect.add(format("select %s %d as step, %s, %s from %s where %s between timestamp '%s' and timestamp '%s' and %s",
                    dimension.map(v -> checkTableColumn(v) + ", ").orElse(""),
                    i,
                    connectorString,
                    checkTableColumn(projectConfig.getTimeColumn()),
                    checkCollection(steps.get(i).getCollection()),
                    checkTableColumn(projectConfig.getTimeColumn()),
                    startDate,
                    endDate.plusDays(1),
                    filterExp.orElse("true")));
        }

        String dimensions = dimension.map(v -> checkTableColumn(v) + ", ").orElse("");
        String query =  format("select %s %s\n" +
                        "from (select %s,\n" +
                        "            %s %s" +
                        "     from (\n" +
                        "     %s" +
                        "     ) t \n" +
                        "     group by %s %s\n" +
                        "    ) t %s",
                dimensions,
                selects.stream().collect(Collectors.joining(",\n")),
                connectorString,
                dimensions,
                insideSelect.stream().collect(Collectors.joining(",\n")),
                mainSelect.stream().collect(Collectors.joining(" UNION ALL\n")),
                dimensions,
                connectorString,
                dimension.map(v -> " group by 1 order by 2 desc").orElse(""));

        if(dimension.isPresent()) {
            query = format("SELECT * FROM (%s) data WHERE event0_count > 0", query);
        }

        QueryExecution queryExecution = executor.executeQuery(project, query, Optional.empty(), "collection", timezone, 1000);

        return new DelegateQueryExecution(queryExecution,
                result -> {
                    if (result.isFailed()) {
                        return result;
                    }

                    List<List<Object>> newResult = new ArrayList<>();
                    List<SchemaField> metadata;

                    if (dimension.isPresent()) {
                        metadata = ImmutableList.of(
                                new SchemaField("step", STRING),
                                new SchemaField("dimension", STRING),
                                new SchemaField("count", LONG));

                        for (List<Object> objects : result.getResult()) {
                            for (int i = 0; i < steps.size(); i++) {
                                newResult.add(ImmutableList.of("Step " + (i + 1),
                                        Optional.ofNullable(objects.get(0)).orElse("(not set)"),
                                        Optional.ofNullable(objects.get(i + 1)).orElse(0)));
                            }
                        }
                    }
                    else {
                        metadata = ImmutableList.of(
                                new SchemaField("step", STRING),
                                new SchemaField("count", LONG));

                        List<Object> stepCount = result.getResult().get(0);
                        for (int i = 0; i < steps.size(); i++) {
                            Object value = stepCount.get(i);
                            newResult.add(ImmutableList.of("Step " + (i + 1), value == null ? 0 : value));
                        }
                    }

                    return new QueryResult(metadata, newResult, result.getProperties());
                });
    }
}
