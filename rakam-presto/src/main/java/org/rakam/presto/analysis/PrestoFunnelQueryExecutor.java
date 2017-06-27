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
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.AbstractFunnelQueryExecutor;
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
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.RakamExpressionFormatter.formatIdentifier;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.rakam.presto.analysis.PrestoUserService.ANONYMOUS_ID_MAPPING;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class PrestoFunnelQueryExecutor
        extends AbstractFunnelQueryExecutor
{
    private final boolean userMappingEnabled;
    private final FastGenericFunnelQueryExecutor fastPrestoFunnelQueryExecutor;
    private final PrestoApproxFunnelQueryExecutor approxFunnelQueryExecutor;

    @Inject
    public PrestoFunnelQueryExecutor(
            ProjectConfig projectConfig,
            FastGenericFunnelQueryExecutor fastPrestoFunnelQueryExecutor,
            PrestoApproxFunnelQueryExecutor approxFunnelQueryExecutor,
            Metastore metastore,
            QueryExecutor executor,
            UserPluginConfig userPluginConfig)
    {
        super(projectConfig, metastore, executor);
        this.fastPrestoFunnelQueryExecutor = fastPrestoFunnelQueryExecutor;
        this.approxFunnelQueryExecutor = approxFunnelQueryExecutor;
        this.userMappingEnabled = userPluginConfig.getEnableUserMapping();
    }

    @Override
    public String getTemplate(List<FunnelStep> steps, Optional<String> dimension, Optional<FunnelWindow> window)
    {
        return "select %s step, count(*) total from (\n" +
                "select %s funnel_step_time(array_agg(cast(step as tinyint)), array_agg(cast(to_unixtime(" + checkTableColumn(projectConfig.getTimeColumn()) + ") as integer))) as step from (select * from (%s) WHERE "
                + checkTableColumn(projectConfig.getTimeColumn()) + " between timestamp '%s' and timestamp '%s'\n" +
                ") t group by %s %s\n" +
                ") t group by 1 %s order by 1";
    }

    @Override
    public QueryExecution query(String project, List<FunnelStep> steps, Optional<String> dimension, LocalDate startDate, LocalDate endDate, Optional<FunnelWindow> window, ZoneId zoneId, Optional<List<String>> connectors, Optional<Boolean> ordered, Optional<Boolean> approximate)
    {
        if (approximate.orElse(false)) {
            return approxFunnelQueryExecutor.query(project, steps, dimension, startDate, endDate, window, zoneId, connectors, ordered, Optional.empty());
        }

        if (!ordered.orElse(false)) {
            return fastPrestoFunnelQueryExecutor.query(project, steps, dimension, startDate, endDate, window, zoneId, connectors, ordered, Optional.empty());
        }

        if (dimension.isPresent() && projectConfig.getUserColumn().equals(dimension.get())) {
            throw new RakamException("Dimension and connector field cannot be equal", HttpResponseStatus.BAD_REQUEST);
        }

        return super.query(project, steps, dimension, startDate, endDate, window, zoneId, connectors, ordered, approximate);
    }

    public String convertFunnel(String project, String connectorField, int idx, FunnelStep funnelStep, Optional<String> dimension, LocalDate startDate, LocalDate endDate)
    {
        Optional<String> filterExp = funnelStep.getExpression().map(value -> RakamSqlFormatter.formatExpression(value,
                name -> name.getParts().stream().map(e -> formatIdentifier(e, '"')).collect(Collectors.joining(".")),
                name -> formatIdentifier("step" + idx, '"') + "." + name.getParts().stream()
                        .map(e -> formatIdentifier(e, '"')).collect(Collectors.joining(".")), '"'));

        String format = format("SELECT %s %s, %d as step, %s.%s from %s %s %s %s",
                dimension.map(ValidationUtil::checkTableColumn).map(v -> "step" + idx + "." + v + ",").orElse(""),
                userMappingEnabled ? format("coalesce(mapping._user, %s._user, %s) as _user", "step" + idx, format(connectorField, "step" + idx)) : connectorField,
                idx + 1,
                "step" + idx,
                checkTableColumn(projectConfig.getTimeColumn()),
                project + "." + checkCollection(funnelStep.getCollection()),
                "step" + idx,
                userMappingEnabled ? format("left join %s.%s mapping on (%s.%s is null and mapping.created_at >= date '%s' and mapping.merged_at <= date '%s' and mapping.id = %s.%s)",
                        project, checkCollection(ANONYMOUS_ID_MAPPING),
                        "step" + idx, checkTableColumn(projectConfig.getUserColumn()), startDate.format(ISO_LOCAL_DATE), endDate.format(ISO_LOCAL_DATE),
                        "step" + idx, checkTableColumn(projectConfig.getUserColumn())) : "",
                filterExp.map(v -> "where " + v).orElse(""));
        return format;
    }
}
