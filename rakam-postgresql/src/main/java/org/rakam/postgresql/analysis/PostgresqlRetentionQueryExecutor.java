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

import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.report.AbstractRetentionQueryExecutor;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;

import javax.inject.Inject;

import static java.lang.String.format;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.WEEK;

public class PostgresqlRetentionQueryExecutor extends AbstractRetentionQueryExecutor {

    @Inject
    public PostgresqlRetentionQueryExecutor(PostgresqlQueryExecutor executor,
                                            Metastore metastore,
                                            MaterializedViewService materializedViewService,
                                            ContinuousQueryService continuousQueryService) {
        super(executor, metastore, materializedViewService, continuousQueryService);
    }

    @Override
    public String diffTimestamps(DateUnit dateUnit, String start, String end) {
        if(dateUnit == WEEK) {
            // Postgresql doesn't support date_part('week', interval).
            return format("cast(date_part('day', age(%s, %s)) as bigint)*7", end, start);
        }
        return format("cast(date_part('%s', age(%s, %s)) as bigint)",
                dateUnit.name().toLowerCase(), end, start);
    }
}

