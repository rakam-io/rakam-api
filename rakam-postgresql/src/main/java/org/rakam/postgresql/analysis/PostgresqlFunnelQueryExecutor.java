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
package org.rakam.postgresql.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.google.common.base.Throwables;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.AbstractFunnelQueryExecutor;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.config.ProjectConfig;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.QueryExecution;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.RakamExpressionFormatter.formatIdentifier;
import static java.lang.String.format;
import static org.rakam.util.ValidationUtil.checkProject;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class PostgresqlFunnelQueryExecutor
        extends AbstractFunnelQueryExecutor
{
    private final PostgresqlQueryExecutor executor;
    private final FastGenericFunnelQueryExecutor fastExecutor;

    @Inject
    public PostgresqlFunnelQueryExecutor(FastGenericFunnelQueryExecutor fastExecutor, ProjectConfig projectConfig, Metastore metastore, PostgresqlQueryExecutor executor)
    {
        super(projectConfig, metastore, executor);
        this.executor = executor;
        this.fastExecutor = fastExecutor;
    }

    @PostConstruct
    public void setup()
    {
        try (Connection conn = executor.getConnection()) {
            conn.createStatement().execute("CREATE OR REPLACE FUNCTION get_funnel_step(arr int[]) RETURNS integer AS $$\n" +
                    "DECLARE next_step integer := 1; step integer;\n" +
                    "        BEGIN \n" +
                    "                FOREACH step IN ARRAY arr\n" +
                    "       LOOP\n" +
                    "      IF step = next_step THEN\n" +
                    "         next_step = next_step + 1;\n" +
                    "      END IF;\n" +
                    "       END LOOP;\n" +
                    "    return next_step - 1;\n" +
                    "        END;\n" +
                    "$$ LANGUAGE plpgsql;");
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public QueryExecution query(String project, List<FunnelStep> steps, Optional<String> dimension, LocalDate startDate, LocalDate endDate, Optional<FunnelWindow> window, ZoneId zoneId, Optional<List<String>> connectors, Optional<Boolean> ordered, Optional<Boolean> approximate)
    {
        if (!ordered.orElse(false)) {
            return fastExecutor.query(project, steps, dimension, startDate, endDate, window, zoneId, connectors, ordered, approximate);
        }

        if (dimension.isPresent() && projectConfig.getUserColumn().equals(dimension.get())) {
            throw new RakamException("Dimension and connector field cannot be equal", HttpResponseStatus.BAD_REQUEST);
        }

        return super.query(project, steps, dimension, startDate, endDate, window, zoneId, connectors, ordered, approximate);
    }

    @Override
    public String getTemplate(List<FunnelStep> steps, Optional<String> dimension, Optional<FunnelWindow> window)
    {
        return "select %s get_funnel_step(steps) step, count(*) total from (\n" +
                "select %s array_agg(step order by " + checkTableColumn(projectConfig.getTimeColumn()) + ") as steps from (%s) t WHERE " + checkTableColumn(projectConfig.getTimeColumn()) + " between timestamp '%s' and timestamp '%s'\n" +
                "group by %s %s\n" +
                ") t group by 1 %s order by 1";
    }

    public String convertFunnel(String project, String connectorField, int idx, FunnelStep funnelStep, Optional<String> dimension, LocalDate startDate, LocalDate endDate)
    {
        String table = checkProject(project, '"') + "." + ValidationUtil.checkCollection(funnelStep.getCollection());
        Optional<String> filterExp = funnelStep.getExpression().map(value -> RakamSqlFormatter.formatExpression(value,
                name -> name.getParts().stream().map(e -> formatIdentifier(e, '"')).collect(Collectors.joining(".")),
                name -> formatIdentifier("step" + idx, '"') + "." + name.getParts().stream()
                        .map(e -> formatIdentifier(e, '"')).collect(Collectors.joining(".")), '"'));

        String format = format("SELECT %s %s, %d as step, %s from %s %s %s",
                dimension.map(ValidationUtil::checkTableColumn).map(v -> v + ",").orElse(""),
                format(connectorField, "step" + idx),
                idx + 1,
                checkTableColumn(projectConfig.getTimeColumn()),
                table,
                "step" + idx,
                filterExp.map(v -> "where " + v).orElse(""));
        return format;
    }
}

