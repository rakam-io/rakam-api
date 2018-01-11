package org.rakam.postgresql.analysis;

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
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class FastGenericFunnelQueryExecutor
        implements FunnelQueryExecutor {
    private final ProjectConfig projectConfig;
    private final QueryExecutorService executor;
    private final Metastore metastore;
    private Map<FunnelTimestampSegments, String> timeStampMapping;


    @Inject
    public FastGenericFunnelQueryExecutor(QueryExecutorService executor, ProjectConfig projectConfig, Metastore metastore) {
        this.projectConfig = projectConfig;
        this.executor = executor;
        this.metastore = metastore;
    }

    @Override
    public QueryExecution query(RequestContext context, List<FunnelStep> steps, Optional<String> dimension, Optional<String> segment, LocalDate startDate, LocalDate endDate, Optional<FunnelWindow> window, ZoneId timezone, Optional<List<String>> connectors, FunnelType funnelType) {
        if (dimension.map(v -> projectConfig.getUserColumn().equals(v)).orElse(false)) {
            throw new RakamException("user column can't be used as dimension", BAD_REQUEST);
        }

        if (segment.isPresent()) {
            if (dimension.isPresent()) {
                SchemaField column = metastore.getCollection(context.project, steps.get(0).getCollection()).stream()
                        .filter(c -> c.getName().equals(dimension.get()))
                        .findAny().orElseThrow(() -> new RakamException("Dimension is not exist.", BAD_REQUEST));
                if (column.getType() != TIMESTAMP) {
                    throw new RakamException("Segment is supported only for TIMESTAMP columns.", BAD_REQUEST);
                }

                if (!timeStampMapping.containsKey(FunnelTimestampSegments.valueOf(segment.get().toUpperCase()))) {
                    throw new RakamException("When dimension is of type TIMESTAMP, segmenting should be done on TIMESTAMP field.", BAD_REQUEST);
                }
            } else {
                throw new RakamException("Dimension can't be null when segment is not.", BAD_REQUEST);
            }
        }

        if (funnelType == FunnelType.ORDERED) {
            throw new RakamException("Strict ordered funnel query is not supported", BAD_REQUEST);
        }

        if (dimension.isPresent() && connectors.isPresent() && connectors.get().contains(dimension)) {
            throw new RakamException("Dimension and connector field cannot be equal", BAD_REQUEST);
        }

        List<String> selects = new ArrayList<>();
        List<String> insideSelect = new ArrayList<>();
        List<String> mainSelect = new ArrayList<>();
        List<String> leasts = new ArrayList();

        String connectorString = connectors.map(item -> item.stream().map(ValidationUtil::checkTableColumn).collect(Collectors.joining(", ")))
                .orElse(checkTableColumn(projectConfig.getUserColumn()));

        for (int i = 0; i < steps.size(); i++) {
            Optional<String> filterExp = steps.get(i).getExpression().map(value -> RakamSqlFormatter.formatExpression(value,
                    name -> name.getParts().stream().map(e -> formatIdentifier(e, '"')).collect(Collectors.joining(".")),
                    ValidationUtil::checkTableColumn, '"'));

            leasts.add(format("least(%s)", IntStream.range(0, i + 1)
                    .mapToObj(idx -> format("event%d_count", idx)).collect(Collectors.joining(","))));

            if (i == 0) {
                selects.add(format("sum(case when ts_event%d is not null then 1 else 0 end) as event%d_count", i, i));
            } else {
                selects.add(format("sum(case when ts_event%d >= ts_event%d then 1 else 0 end) as event%d_count", i, i - 1, i));
            }

            insideSelect.add(format("min(case when step = %d then %s end) as ts_event%d", i, checkTableColumn(projectConfig.getTimeColumn()), i));
            mainSelect.add(format("select %s %s %d as step, %s %s from %s where %s between timestamp '%s' and timestamp '%s' and %s",
                    dimension.map(v -> checkTableColumn(v) + ", ").orElse(""),
                    segment.isPresent() ? format(timeStampMapping.get(FunnelTimestampSegments.valueOf(segment.get().replace(" ", "_").toUpperCase())),
                            dimension.get()) + " as " + checkTableColumn(dimension.get() + "_segment") + "," : "",
                    i,
                    connectorString,
                    segment.isPresent() ? "" : ", " + checkTableColumn(projectConfig.getTimeColumn()),
                    checkCollection(steps.get(i).getCollection()),
                    checkTableColumn(projectConfig.getTimeColumn()),
                    startDate,
                    endDate.plusDays(1),
                    filterExp.orElse("true")));
        }

        String dimensions = "";

        if (segment.isPresent()) {
            dimensions = checkTableColumn(projectConfig.getTimeColumn() + "_segment") + ", ";
        } else if (dimension.isPresent()) {
            dimensions = dimension.map(v -> checkTableColumn(v) + ", ").orElse(""); // outer dimensions
        }

        String query = format("select %s %s\n" +
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

        String collect = leasts.stream().collect(Collectors.joining(", "));
        if (dimension.isPresent() || segment.isPresent()) {
            query = format("SELECT %s %s FROM (%s) data WHERE event0_count > 0", dimensions, collect, query);
        } else {
            query = format("SELECT %s FROM (%s) data", collect, query);
        }

        QueryExecution queryExecution = executor.executeQuery(context, query, Optional.empty(), null, timezone, 1000);

        return new DelegateQueryExecution(queryExecution,
                result -> {
                    if (result.isFailed()) {
                        return result;
                    }

                    List<List<Object>> newResult = new ArrayList<>();
                    List<SchemaField> metadata;

                    if (dimension.isPresent() || segment.isPresent()) {
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
                    } else {
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

    public void setTimeStampMapping(Map<FunnelTimestampSegments, String> timeStampMapping) {
        this.timeStampMapping = timeStampMapping;
    }
}
