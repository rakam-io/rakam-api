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
package org.rakam.report;

import org.rakam.analysis.FunnelAnalysis;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/08/15 15:27.
 */
public class PrestoFunnelQueryExecutor implements FunnelAnalysis {
    private final PrestoQueryExecutor executor;
    private final String coldStorageConnector;

    public PrestoFunnelQueryExecutor(PrestoQueryExecutor executor, PrestoConfig config) {
        this.executor = executor;
        coldStorageConnector = config.getColdStorageConnector();
    }

    @Override
    public QueryExecution query(String project, List<FunnelStep> steps, Optional<String> dimension) {
        String ctes = IntStream.range(0, steps.size())
                .mapToObj(i -> convertFunnel(project, i, steps.get(i)))
                .collect(Collectors.joining(", "));
        String query = ctes + " " + IntStream.range(0, steps.size())
                .mapToObj(i -> "select count(*) from step%s" + i)
                .collect(Collectors.joining(" union all "));
        return executor.executeRawQuery(query);
    }

    private String convertFunnel(String project, int idx, FunnelStep funnelStep) {
        String table = coldStorageConnector + "." + project + "." + funnelStep.collection();

        if(idx == 0) {
            return String.format("step%s AS (\n" +
                    "select \"user\" from %s where %s\n group by 1" +
                    ")", idx, table, funnelStep.filterExpression());
        } else {
            return String.format("%1$s AS (\n" +
                    "select %1$s.\"user\" from %2$s %1$s join %3$s on (%1$s.\"user\" = %3$s.\"user\") where %4$s group by 1\n" +
                    ")", "step"+idx, table, "step"+(idx-1), funnelStep.filterExpression());
        }
    }
}
