package org.rakam.presto.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.google.common.collect.ImmutableList;
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
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.RakamExpressionFormatter.formatIdentifier;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static org.rakam.collection.FieldType.LONG;
import static org.rakam.collection.FieldType.STRING;
import static org.rakam.util.DateTimeUtils.TIMESTAMP_FORMATTER;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class PrestoApproxFunnelQueryExecutor
        implements FunnelQueryExecutor
{
    private final ProjectConfig projectConfig;
    private final QueryExecutorService executor;

    @Inject
    public PrestoApproxFunnelQueryExecutor(ProjectConfig projectConfig, QueryExecutorService executor)
    {
        this.projectConfig = projectConfig;
        this.executor = executor;
    }

    @Override
    public QueryExecution query(String project, List<FunnelStep> steps, Optional<String> dimension, LocalDate startDate, LocalDate endDate, Optional<FunnelWindow> window, ZoneId zoneId, Optional<List<String>> connectors, FunnelType funnelType)
    {
        String startDateStr = TIMESTAMP_FORMATTER.format(startDate.atStartOfDay(zoneId));
        String endDateStr = TIMESTAMP_FORMATTER.format(endDate.plusDays(1).atStartOfDay(zoneId));
        if (funnelType == FunnelType.ORDERED) {
            throw new RakamException("Ordered funnel query is not supported when approximation is enabled.", BAD_REQUEST);
        }

        if (window.isPresent()) {
            throw new RakamException("Windowed funnel query is not supported when approximation is enabled.", BAD_REQUEST);
        }

        String query;
        if (dimension.isPresent()) {
            String queries = IntStream.range(0, steps.size()).mapToObj(step -> String.format("(select %s as dimension, %s as _user, %d as step from %s where %s between timestamp '%s' and timestamp '%s' and %s)",
                    checkTableColumn(dimension.get()), checkTableColumn(projectConfig.getUserColumn()), step,
                    checkCollection(steps.get(step).getCollection()),
                    checkTableColumn(projectConfig.getTimeColumn()),
                    startDateStr, endDateStr,
                    getFilterExp(steps.get(step)))).collect(Collectors.joining(" union all "));

            query = format("select step, dimension, approx_funnel(user_sets) OVER (PARTITION BY dimension ORDER BY step) as count from (select dimension, step, approx_set(_user) user_sets from (%s) where dimension is not null group by 1, 2)", queries);
        }
        else {
            String queries = steps.stream().map(step -> String.format("(select approx_set(%s) from %s where %s between timestamp '%s' and timestamp '%s' and %s )",
                    checkTableColumn(projectConfig.getUserColumn()),
                    checkCollection(step.getCollection()),
                    checkTableColumn(projectConfig.getTimeColumn()),
                    startDateStr, endDateStr, getFilterExp(step)))
                    .collect(Collectors.joining(", "));

            query = String.format("select funnel_steps(array[%s])", queries);
        }

        QueryExecution queryExecution = executor.executeQuery(project, query, Optional.empty(), "collection", zoneId, 10000);

        return new DelegateQueryExecution(queryExecution,
                result -> {
                    if (result.isFailed()) {
                        return result;
                    }

                    List<List<Object>> newResult = new ArrayList<>();
                    List<SchemaField> metadata;

                    if (dimension.isPresent()) {
                        metadata = result.getMetadata();
                        newResult = result.getResult();

                        for (List<Object> objects : result.getResult()) {
                            objects.set(0, "Step "+objects.get(0));
                        }
                    }
                    else {
                        metadata = ImmutableList.of(
                                new SchemaField("step", STRING),
                                new SchemaField("count", LONG));

                        ArrayList<Long> step = (ArrayList<Long>) result.getResult().get(0).get(0);
                        for (int i = 0; i < step.size(); i++) {
                            Object value = step.get(i);
                            newResult.add(ImmutableList.of("Step " + (i + 1), value == null ? 0 : value));
                        }
                    }

                    return new QueryResult(metadata, newResult, result.getProperties());
                });
    }

    private String getFilterExp(FunnelStep step)
    {
        return step.getExpression().map(value -> RakamSqlFormatter.formatExpression(value,
                name -> name.getParts().stream().map(e -> formatIdentifier(e, '"')).collect(Collectors.joining(".")),
                ValidationUtil::checkTableColumn, '"')).orElse("true");
    }
}
