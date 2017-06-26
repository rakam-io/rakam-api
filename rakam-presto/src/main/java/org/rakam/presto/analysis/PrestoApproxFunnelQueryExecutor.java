package org.rakam.presto.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.config.ProjectConfig;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.RakamExpressionFormatter.formatIdentifier;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
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
    public QueryExecution query(String project, List<FunnelStep> steps, Optional<String> dimension, LocalDate startDate, LocalDate endDate, Optional<FunnelWindow> window, ZoneId zoneId, Optional<List<String>> connectors, Optional<Boolean> ordered, Optional<Boolean> approximate)
    {
        String startDateStr = TIMESTAMP_FORMATTER.format(startDate.atStartOfDay(zoneId));
        String endDateStr = TIMESTAMP_FORMATTER.format(endDate.plusDays(1).atStartOfDay(zoneId));
        if (ordered.isPresent() && ordered.get()) {
            throw new RakamException("Ordered funnel query is not supported when approximation is enabled.", BAD_REQUEST);
        }

        if (window.isPresent()) {
            throw new RakamException("Windowed funnel query is not supported when approximation is enabled.", BAD_REQUEST);
        }

        List<String> withs = new ArrayList<>();
        for (int i = 0; i < steps.size(); i++) {
            FunnelStep funnelStep = steps.get(i);

            String filterExp = funnelStep.getExpression().map(value -> RakamSqlFormatter.formatExpression(value,
                    name -> name.getParts().stream().map(e -> formatIdentifier(e, '"')).collect(Collectors.joining(".")),
                    name -> name.getParts().stream()
                            .map(e -> formatIdentifier(e, '"')).collect(Collectors.joining(".")), '"')).orElse("true");

            String withStep = format("step%d as (select %s as set %s from %s %s where %s between timestamp '%s' and timestamp '%s' and %s)",
                    i, i == 0 ? format("approx_set(%s)", checkCollection(projectConfig.getUserColumn())) : format("merge_sets(step%d.set, approx_set(%s))", i - 1, checkTableColumn(projectConfig.getUserColumn())),
                    dimension.map(v -> ", " + checkTableColumn(v)).orElse(""),
                    checkCollection(funnelStep.getCollection()),
                    i > 0 ? format(", step%s", i - 1) : "",
                    checkTableColumn(projectConfig.getTimeColumn()),
                    startDateStr, endDateStr, filterExp);
            withs.add(withStep);
        }

        String querySelect = IntStream.range(0, steps.size())
                .mapToObj(i -> format("cardinality(step%d.set)", i))
                .collect(Collectors.joining(", "));

        String queryEnd = IntStream.range(0, steps.size())
                .mapToObj(i -> format("step%d", i))
                .collect(Collectors.joining(", "));

        return executor.executeQuery(project, format("with \n%s\n select %s %s from %s %s",
                withs.stream().collect(Collectors.joining(",\n")),
                dimension.map(v -> checkTableColumn(v) + ", ").orElse(""),
                querySelect, queryEnd, dimension.map(v -> " group by 1 ").orElse("")));
    }
}
