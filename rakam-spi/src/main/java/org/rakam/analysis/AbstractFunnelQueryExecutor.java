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

import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.util.RakamException;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.of;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static org.rakam.collection.FieldType.LONG;
import static org.rakam.collection.FieldType.STRING;
import static org.rakam.util.DateTimeUtils.TIMESTAMP_FORMATTER;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public abstract class AbstractFunnelQueryExecutor
        implements FunnelQueryExecutor {
    protected final ProjectConfig projectConfig;
    private final QueryExecutor executor;
    private final Metastore metastore;
    protected Map<FunnelTimestampSegments, String> timeStampMapping;

    public AbstractFunnelQueryExecutor(ProjectConfig projectConfig, Metastore metastore, QueryExecutor executor) {
        this.projectConfig = projectConfig;
        this.metastore = metastore;
        this.executor = executor;
    }

    public abstract String getTemplate(List<FunnelStep> steps, Optional<String> dimension, Optional<FunnelWindow> window);

    public abstract String convertFunnel(String project, String connectorField, int idx, FunnelStep funnelStep, Optional<String> dimension, Optional<String> segment, LocalDate startDate, LocalDate endDate);

    @Override
    public QueryExecution query(RequestContext context,
                                List<FunnelStep> steps,
                                Optional<String> dimension,
                                Optional<String> segment,
                                LocalDate startDate,
                                LocalDate endDate, Optional<FunnelWindow> window, ZoneId timezone,
                                Optional<List<String>> connectors,
                                FunnelType type) {
        Map<String, List<SchemaField>> collections = metastore.getCollections(context.project);

        if (dimension.isPresent()) {
            if (dimension.get().equals(projectConfig.getTimeColumn())) {
                if (!segment.isPresent() || !timeStampMapping.containsKey(FunnelTimestampSegments.valueOf(segment.get().toUpperCase()))) {
                    throw new RakamException("When dimension is time, segmenting should be done on timestamp field.", BAD_REQUEST);
                }
            }
            if (metastore.getCollections(context.project).entrySet().stream()
                    .filter(c -> !c.getValue().contains(dimension.get())).findAny().get().getValue().stream()
                    .filter(d -> d.getName().equals(dimension.get())).findAny().get().getType().getPrettyName().equals("TIMESTAMP")) {
                if (!segment.isPresent() || !timeStampMapping.containsKey(FunnelTimestampSegments.valueOf(segment.get().toUpperCase()))) {
                    throw new RakamException("When dimension is of type TIMESTAMP, segmenting should be done on timestamp field.", BAD_REQUEST);
                }
            }
        } else if (segment.isPresent()) {
            throw new RakamException("Dimension can't be null when segment is not.", BAD_REQUEST);
        }


        String ctes = IntStream.range(0, steps.size())
                .mapToObj(i -> convertFunnel(
                        context.project, connectors.orElse(ImmutableList.of(testDeviceIdExists(steps.get(i), collections) ? ("coalesce(cast(%s." + checkTableColumn(projectConfig.getUserColumn()) + " as varchar), _device_id) as " + checkTableColumn(projectConfig.getUserColumn())) : projectConfig.getUserColumn()))
                                .stream().collect(Collectors.joining(", ")), i,
                        steps.get(i), dimension, segment, startDate, endDate))
                .collect(Collectors.joining(" UNION ALL "));

        String segment2 = segment.map(v -> "_segment").orElse("");
        String dimensionCol = dimension.map(v -> checkTableColumn(v + segment2) + ", ").orElse("");
        String query = format(getTemplate(steps, dimension, window), dimensionCol, dimensionCol, ctes,
                TIMESTAMP_FORMATTER.format(startDate.atStartOfDay()),
                TIMESTAMP_FORMATTER.format(endDate.plusDays(1).atStartOfDay()),
                dimensionCol,
                connectors.orElse(of(projectConfig.getUserColumn()))
                        .stream().collect(Collectors.joining(", ")),
                dimension.map(v -> ", 2").orElse(""));
        if (dimension.isPresent()) {
            query = String.format("SELECT (CASE WHEN rank > 15 THEN 'Others' ELSE cast(%s as varchar) END) as dimension, step, sum(total) from " +
                            "(select *, row_number() OVER(ORDER BY total DESC) rank from (%s) t) t GROUP BY 1, 2",
                    dimension.map(v -> checkTableColumn(v + segment2)).get(), query);
        }
        QueryExecution queryExecution = executor.executeRawQuery(context, query, timezone);

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

                        metadata = of(
                                new SchemaField("step", STRING),
                                new SchemaField("dimension", STRING),
                                new SchemaField("count", LONG));
                    } else {
                        newResult = IntStream.range(0, steps.size())
                                .mapToObj(i -> Arrays.<Object>asList("Step " + (i + 1), 0L))
                                .collect(Collectors.toList());

                        for (int step = 0; step < newResult.size(); step++) {
                            int finalStep = step;
                            queryResult.stream().filter(e -> ((Number) e.get(0)).intValue() >= finalStep + 1).map(e -> e.get(1))
                                    .forEach(val -> newResult.get(finalStep).set(1,
                                            ((Number) newResult.get(finalStep).get(1)).longValue() + ((Number) val).longValue()));
                        }

                        metadata = of(
                                new SchemaField("step", STRING),
                                new SchemaField("count", LONG));
                    }
                    return new QueryResult(metadata, newResult, result.getProperties());
                });
    }

    protected boolean testDeviceIdExists(FunnelStep firstAction, Map<String, List<SchemaField>> collections) {
        List<SchemaField> schemaFields = collections.get(firstAction.getCollection());
        if (schemaFields == null) {
            throw new RakamException("The collection in first action does not exist.", HttpResponseStatus.BAD_REQUEST);
        }
        return schemaFields.stream().anyMatch(e -> e.getName().equals("_device_id"));
    }
}
