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
package org.rakam.report.postgresql;

import com.google.common.collect.ImmutableMap;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.report.AbstractEventExplorer;
import org.rakam.report.QueryExecutorService;

import javax.inject.Inject;
import java.util.Map;

import static org.rakam.analysis.EventExplorer.TimestampTransformation.*;

public class PostgresqlEventExplorer extends AbstractEventExplorer {
    private static final Map<TimestampTransformation, String> timestampMapping = ImmutableMap.
            <TimestampTransformation, String>builder()
            .put(HOUR_OF_DAY, "extract(hour from %s)")
            .put(DAY_OF_MONTH, "extract(day FROM %s)")
            .put(WEEK_OF_YEAR, "extract(doy FROM %s)")
            .put(MONTH_OF_YEAR, "extract(month FROM %s)")
            .put(QUARTER_OF_YEAR, "extract(quarter FROM %s)")
            .put(DAY_OF_WEEK, "extract(dow FROM %s)")
            .put(HOUR, "date_trunc('hour', %s)")
            .put(DAY, "cast(%s as date)")
            .put(MONTH, "date_trunc('month', %s)")
            .put(YEAR, "date_trunc('year', %s)")
            .build();

    @Inject
    public PostgresqlEventExplorer(QueryExecutorService service, PostgresqlQueryExecutor executor, Metastore metastore) {
        super(executor, service, metastore, timestampMapping);
    }
}