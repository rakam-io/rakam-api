package org.rakam.presto.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.google.common.collect.ImmutableList;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.analysis.RequestContext;
import org.rakam.analysis.metadata.Metastore;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.RakamExpressionFormatter.formatIdentifier;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static org.rakam.collection.FieldType.*;
import static org.rakam.util.DateTimeUtils.TIMESTAMP_FORMATTER;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class PrestoApproxFunnelQueryExecutor
        implements FunnelQueryExecutor {
    private final ProjectConfig projectConfig;
    private final QueryExecutorService executor;
    private final Metastore metastore;
    private Map<FunnelTimestampSegments, String> timeStampMapping;

    @Inject
    public PrestoApproxFunnelQueryExecutor(ProjectConfig projectConfig, QueryExecutorService executor, Metastore metastore) {
        this.projectConfig = projectConfig;
        this.executor = executor;
        this.metastore = metastore;
    }

    @Override
    public QueryExecution query(RequestContext context, List<FunnelStep> steps, Optional<String> dimension, Optional<String> segment, LocalDate startDate, LocalDate endDate, Optional<FunnelWindow> window, ZoneId zoneId, Optional<List<String>> connectors, FunnelType funnelType) {

        if (dimension.isPresent()) {
            String val = dimension.get();
            if (val.equals(projectConfig.getTimeColumn())) {
                if (!segment.isPresent() || !timeStampMapping.containsKey(FunnelTimestampSegments.valueOf(segment.get().toUpperCase()))) {
                    throw new RakamException("When dimension is time, segmenting should be done on timestamp field.", BAD_REQUEST);
                }
            }
            Optional<SchemaField> fieldType = metastore.getCollections(context.project).entrySet().stream()
                    .filter(c -> !c.getValue().contains(val)).findAny().get().getValue().stream()
                    .filter(d -> d.getName().equals(val)).findAny();
            if(!fieldType.isPresent()) {
                throw new RakamException("Dimension does not exist.", BAD_REQUEST);
            }

            if (fieldType.get().getType().getPrettyName().equals(TIMESTAMP.getPrettyName())) {
                if (!segment.isPresent() || !timeStampMapping.containsKey(FunnelTimestampSegments.valueOf(segment.get().toUpperCase()))) {
                    throw new RakamException("When dimension is of type TIMESTAMP, segmenting should be done on timestamp field.", BAD_REQUEST);
                }
            }
        } else if (segment.isPresent()) {
            throw new RakamException("Dimension can't be null when segment is not.", BAD_REQUEST);
        }

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
                    dimension.map(ValidationUtil::checkTableColumn).map(v -> segment.isPresent() ? applySegment(v, segment) : v).orElse(""),
                    checkTableColumn(projectConfig.getUserColumn()), step,
                    checkCollection(steps.get(step).getCollection()),
                    checkTableColumn(projectConfig.getTimeColumn()),
                    startDateStr, endDateStr,
                    getFilterExp(steps.get(step)))).collect(Collectors.joining(" union all "));

            query = format("select step, dimension, cast(approx_funnel(user_sets) OVER (PARTITION BY dimension ORDER BY step) as bigint) as count from (select dimension, step, approx_set(_user) user_sets from (%s) where dimension is not null group by 1, 2)", queries);
        } else {
            String queries = steps.stream().map(step -> String.format("(select approx_set(%s) from %s where %s between timestamp '%s' and timestamp '%s' and %s )",
                    checkTableColumn(projectConfig.getUserColumn()),
                    checkCollection(step.getCollection()),
                    checkTableColumn(projectConfig.getTimeColumn()),
                    startDateStr, endDateStr, getFilterExp(step)))
                    .collect(Collectors.joining(", "));

            query = String.format("select funnel_steps(array[%s])", queries);
        }

        QueryExecution queryExecution = executor.executeQuery(context, query, Optional.empty(), null, zoneId, 10000);

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
                            objects.set(0, "Step " + ((int) objects.get(0) + 1));
                        }
                    } else {
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

    private String getFilterExp(FunnelStep step) {
        return step.getExpression().map(value -> RakamSqlFormatter.formatExpression(value,
                name -> name.getParts().stream().map(e -> formatIdentifier(e, '"')).collect(Collectors.joining(".")),
                ValidationUtil::checkTableColumn, '"')).orElse("true");
    }

    public void setTimeStampMapping(Map<FunnelTimestampSegments, String> timeStampMapping) {
        this.timeStampMapping = timeStampMapping;
    }

    private String applySegment(String v, Optional<String> segment) {
        return String.format(timeStampMapping.get(FunnelTimestampSegments.valueOf(segment.get().replace(" ", "_").toUpperCase())), v);
    }
}
