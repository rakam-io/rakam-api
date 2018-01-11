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
package org.rakam.presto.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.AbstractFunnelQueryExecutor;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.analysis.RequestContext;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.postgresql.analysis.FastGenericFunnelQueryExecutor;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import javax.inject.Inject;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.RakamExpressionFormatter.formatIdentifier;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.rakam.analysis.FunnelQueryExecutor.FunnelTimestampSegments.*;
import static org.rakam.presto.analysis.PrestoUserService.ANONYMOUS_ID_MAPPING;
import static org.rakam.util.ValidationUtil.*;

public class PrestoFunnelQueryExecutor
        extends AbstractFunnelQueryExecutor {
    private static final Map<FunnelTimestampSegments, String> timeStampMapping = ImmutableMap.
            <FunnelQueryExecutor.FunnelTimestampSegments, String>builder()
            .put(HOUR_OF_DAY, "lpad(cast(hour(%s) as varchar), 2, '0')||':00'")
            .put(DAY_OF_MONTH, "cast(day(%s) as varchar)||'th day'")
            .put(WEEK_OF_YEAR, "cast(week(%s) as varchar)||'th week'")
            .put(MONTH_OF_YEAR, "date_format(%s, '%%M')")
            .put(QUARTER_OF_YEAR, "cast(quarter(%s) as varchar)||'th quarter'")
            .put(DAY_OF_WEEK, "date_format(%s, '%%W')")
            .put(HOUR, "date_trunc('hour', %s)")
            .put(DAY, "cast(%s as date)")
            .put(WEEK, "cast(date_trunc('week', %s) as date)")
            .put(MONTH, "cast(date_trunc('month', %s) as date)")
            .put(YEAR, "cast(date_trunc('year', %s) as date)")
            .build();
    private final boolean userMappingEnabled;
    private final FastGenericFunnelQueryExecutor fastPrestoFunnelQueryExecutor;
    private final PrestoApproxFunnelQueryExecutor approxFunnelQueryExecutor;
    private final PrestoConfig prestoConfig;

    @Inject
    public PrestoFunnelQueryExecutor(
            ProjectConfig projectConfig,
            PrestoConfig prestoConfig,
            FastGenericFunnelQueryExecutor fastPrestoFunnelQueryExecutor,
            PrestoApproxFunnelQueryExecutor approxFunnelQueryExecutor,
            Metastore metastore,
            QueryExecutor executor,
            UserPluginConfig userPluginConfig) {
        super(projectConfig, metastore, executor);
        this.prestoConfig = prestoConfig;
        this.fastPrestoFunnelQueryExecutor = fastPrestoFunnelQueryExecutor;
        this.approxFunnelQueryExecutor = approxFunnelQueryExecutor;
        this.userMappingEnabled = userPluginConfig.getEnableUserMapping();
        this.fastPrestoFunnelQueryExecutor.setTimeStampMapping(timeStampMapping);
        this.approxFunnelQueryExecutor.setTimeStampMapping(timeStampMapping);
        super.timeStampMapping = timeStampMapping;
    }

    @Override
    public String getTemplate(List<FunnelStep> steps, Optional<String> dimension, Optional<FunnelWindow> window) {
        return "select %s step, count(*) total from (\n" +
                "select %s funnel_step_time(array_agg(cast(step as tinyint)), array_agg(cast(to_unixtime(" + checkTableColumn(projectConfig.getTimeColumn()) + ") as integer))) as step from (select * from (%s) WHERE "
                + checkTableColumn(projectConfig.getTimeColumn()) + " between timestamp '%s' and timestamp '%s'\n" +
                ") t group by %s %s\n" +
                ") t group by 1 %s order by 1";
    }

    @Override
    public QueryExecution query(RequestContext context, List<FunnelStep> steps, Optional<String> dimension, Optional<String> segment, LocalDate startDate, LocalDate endDate, Optional<FunnelWindow> window, ZoneId zoneId, Optional<List<String>> connectors, FunnelType funnelType) {
        if (funnelType == FunnelType.APPROXIMATE) {
            return approxFunnelQueryExecutor.query(context, steps, dimension, segment, startDate, endDate, window, zoneId, connectors, funnelType);
        }

        if (funnelType != FunnelType.ORDERED) {
            return fastPrestoFunnelQueryExecutor.query(context, steps, dimension, segment, startDate, endDate, window, zoneId, connectors, funnelType);
        }

        if (dimension.isPresent() && projectConfig.getUserColumn().equals(dimension.get())) {
            throw new RakamException("Dimension and connector field cannot be equal", HttpResponseStatus.BAD_REQUEST);
        }

        return super.query(context, steps, dimension, segment, startDate, endDate, window, zoneId, connectors, funnelType);
    }

    public String convertFunnel(String project, String connectorField, int idx, FunnelStep funnelStep, Optional<String> dimension, Optional<String> segment, LocalDate startDate, LocalDate endDate) {
        Optional<String> filterExp = funnelStep.getExpression().map(value -> RakamSqlFormatter.formatExpression(value,
                name -> name.getParts().stream().map(e -> formatIdentifier(e, '"')).collect(Collectors.joining(".")),
                name -> formatIdentifier("step" + idx, '"') + "." + name, '"'));

        String format = format("SELECT %s %s, %d as step, %s.%s from %s.%s.%s %s %s %s",
                dimension.map(ValidationUtil::checkTableColumn).map(v -> "step" + idx + "." + v).map(v -> segment.isPresent() ? applySegment(v, segment) + " as \"" + dimension.orElse("") + "_segment\"" + "," : v + ",").orElse(""),
                userMappingEnabled ? format("coalesce(mapping._user, %s._user, %s) as _user", "step" + idx, format(connectorField, "step" + idx)) : format(connectorField, "step" + idx),
                idx + 1,
                "step" + idx, checkTableColumn(projectConfig.getTimeColumn()),
                prestoConfig.getColdStorageConnector(), checkProject(project, '"'), checkCollection(funnelStep.getCollection()),
                "step" + idx,
                userMappingEnabled ? format("left join %s.%s mapping on (%s.%s is null and mapping.created_at >= date '%s' and mapping.merged_at <= date '%s' and mapping.id = %s.%s)",
                        project, checkCollection(ANONYMOUS_ID_MAPPING),
                        "step" + idx, checkTableColumn(projectConfig.getUserColumn()), startDate.format(ISO_LOCAL_DATE), endDate.format(ISO_LOCAL_DATE),
                        "step" + idx, checkTableColumn(projectConfig.getUserColumn())) : "",
                filterExp.map(v -> "where " + v).orElse(""));
        return format;
    }

    private String applySegment(String v, Optional<String> segment) {
        return String.format(timeStampMapping.get(FunnelTimestampSegments.valueOf(segment.get().replace(" ", "_").toUpperCase())), v);
    }
}
